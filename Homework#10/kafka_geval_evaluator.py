import argparse
import json
import os
from typing import Any, Dict, Optional

from kafka import KafkaConsumer
from deepeval.metrics import GEval
from deepeval.models import OllamaModel
from deepeval.test_case import LLMTestCase, LLMTestCaseParams

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
JUDGE_MODEL_NAME = os.getenv("OLLAMA_MODEL_NAME", "llama3.1")
OLLAMA_BASE_URL = os.getenv("LOCAL_MODEL_BASE_URL", "http://localhost:11434")
CONSUMER_TIMEOUT_MS = int(os.getenv("CONSUMER_TIMEOUT_MS", "5000"))


def build_consumer(*topics: str) -> KafkaConsumer:
    return KafkaConsumer(
        *topics,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
    )


def normalize_text(payload: Dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, list):
            return "\n".join(str(x) for x in value)
    return ""


def read_messages_for_question(question_id: str) -> Dict[str, Optional[Dict[str, Any]]]:
    matched: Dict[str, Optional[Dict[str, Any]]] = {
        "tasks": None,
        "drafts": None,
        "final": None,
    }

    consumer = build_consumer("tasks", "drafts", "final")
    try:
        for message in consumer:
            payload = message.value
            if payload.get("question_id") != question_id:
                continue
            matched[message.topic] = payload
    finally:
        consumer.close()

    return matched


def build_metrics(judge_model: OllamaModel):
    plan_quality = GEval(
        name="Plan Quality",
        criteria=(
            "Evaluate whether the planner output is a clear, logical, and actionable "
            "plan for solving the user request. Reward correct decomposition, useful "
            "ordering, completeness, and relevance to the input."
        ),
        evaluation_params=[
            LLMTestCaseParams.INPUT,
            LLMTestCaseParams.ACTUAL_OUTPUT,
        ],
        model=judge_model,
    )

    helpfulness = GEval(
        name="Helpfulness",
        criteria=(
            "Evaluate whether the answer is relevant, correct, complete, and useful "
            "for the user request. Penalize vagueness, missing key details, or "
            "irrelevant filler."
        ),
        evaluation_params=[
            LLMTestCaseParams.INPUT,
            LLMTestCaseParams.ACTUAL_OUTPUT,
        ],
        model=judge_model,
    )

    improvement = GEval(
        name="Final-vs-Draft Improvement",
        criteria=(
            "Given the writer draft in CONTEXT and the reviewer final answer as "
            "ACTUAL_OUTPUT, determine whether the final answer is better than the "
            "draft in correctness, clarity, completeness, and usefulness while "
            "remaining faithful to the user request."
        ),
        evaluation_params=[
            LLMTestCaseParams.INPUT,
            LLMTestCaseParams.ACTUAL_OUTPUT,
            LLMTestCaseParams.CONTEXT,
        ],
        model=judge_model,
    )

    return plan_quality, helpfulness, improvement


def evaluate_messages(user_input: str, plan: str, draft: str, final_answer: str) -> Dict[str, Dict[str, Any]]:
    judge_model = OllamaModel(
        model=JUDGE_MODEL_NAME,
        base_url=OLLAMA_BASE_URL,
        temperature=0,
    )

    plan_quality_metric, helpfulness_metric, improvement_metric = build_metrics(judge_model)

    plan_case = LLMTestCase(
        input=user_input,
        actual_output=plan,
    )
    draft_case = LLMTestCase(
        input=user_input,
        actual_output=draft,
    )
    final_case = LLMTestCase(
        input=user_input,
        actual_output=final_answer,
        context=[draft],
    )

    plan_quality_metric.measure(plan_case)
    helpfulness_metric.measure(draft_case)
    draft_score = helpfulness_metric.score
    draft_reason = helpfulness_metric.reason

    helpfulness_metric.measure(final_case)
    final_score = helpfulness_metric.score
    final_reason = helpfulness_metric.reason

    improvement_metric.measure(final_case)

    return {
        "planner": {
            "metric": "Plan Quality",
            "score": float(plan_quality_metric.score),
            "reason": plan_quality_metric.reason,
            "text": plan,
        },
        "writer": {
            "metric": "Helpfulness",
            "score": float(draft_score),
            "reason": draft_reason,
            "text": draft,
        },
        "reviewer": {
            "metric": "Helpfulness",
            "score": float(final_score),
            "reason": final_reason,
            "text": final_answer,
        },
        "improvement": {
            "metric": "Final-vs-Draft Improvement",
            "score": float(improvement_metric.score),
            "reason": improvement_metric.reason,
            "draft": draft,
            "final": final_answer,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate Planner, Writer, and Reviewer outputs with GEval."
    )
    parser.add_argument(
        "--question-id",
        required=True,
        help="Shared question_id of the Kafka workflow to evaluate.",
    )
    args = parser.parse_args()

    messages = read_messages_for_question(args.question_id)

    if not all(messages.values()):
        missing = [topic for topic, payload in messages.items() if payload is None]
        raise RuntimeError(f"Missing messages for question_id={args.question_id}: {missing}")

    task_msg = messages["tasks"] or {}
    draft_msg = messages["drafts"] or {}
    final_msg = messages["final"] or {}

    user_input = normalize_text(task_msg, ["question", "input", "user_input", "prompt"])
    plan = normalize_text(task_msg, ["plan", "tasks", "outline"])
    draft = normalize_text(draft_msg, ["draft", "answer", "content"])
    final_answer = normalize_text(final_msg, ["final_answer", "answer", "content"])

    if not user_input or not plan or not draft or not final_answer:
        raise RuntimeError(
            "Could not extract user_input, plan, draft, or final_answer from Kafka messages. "
            "Check your message field names."
        )

    results = evaluate_messages(user_input, plan, draft, final_answer)

    print("\nGEval Results")
    print("=" * 60)
    print(f"Question ID: {args.question_id}\n")

    for stage, info in results.items():
        print(f"[{stage.upper()}] {info['metric']}")
        print(f"Score : {info['score']:.3f}")
        print(f"Reason: {info['reason']}\n")

    output_file = f"geval_results_{args.question_id}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"Saved results to {output_file}")


if __name__ == "__main__":
    main()