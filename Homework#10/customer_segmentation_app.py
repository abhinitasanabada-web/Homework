import os
import pickle

import matplotlib.pyplot as plt
import numpy as np
import streamlit as st

MODEL_FILE = 'customer_kmeans_model_abhinita.pkl'
METADATA_FILE = 'customer_metadata.pkl'


@st.cache_resource

def load_resources(model_file: str, metadata_file: str):
    if not os.path.exists(model_file) or not os.path.exists(metadata_file):
        raise FileNotFoundError(
            "Required .pkl files are missing. Run train_customer_model.py first."
        )

    with open(model_file, 'rb') as f:
        model = pickle.load(f)

    with open(metadata_file, 'rb') as f:
        metadata = pickle.load(f)

    return model, metadata


def build_scatter_plot(data: np.ndarray, model, customer_point: np.ndarray):
    labels = model.predict(data)
    fig, ax = plt.subplots(figsize=(8, 5))

    ax.scatter(
        data[:, 0],
        data[:, 1],
        c=labels,
        alpha=0.65,
        edgecolors='none',
        label='Simulated customers'
    )

    centers = model.cluster_centers_
    ax.scatter(
        centers[:, 0],
        centers[:, 1],
        marker='X',
        s=220,
        c='black',
        label='Cluster centers'
    )

    ax.scatter(
        customer_point[0, 0],
        customer_point[0, 1],
        marker='*',
        s=300,
        c='red',
        edgecolors='black',
        label='Predicted customer'
    )

    ax.set_xlabel('Annual Income (k$)')
    ax.set_ylabel('Spending Score (1-100)')
    ax.set_title('Customer Segmentation with K-Means (k=5)')
    ax.legend()
    ax.grid(True, alpha=0.25)
    return fig


def main() -> None:
    st.set_page_config(page_title='Customer Segmentation', layout='wide')
    st.title('Customer Segmentation using K-Means')
    st.write('Predict the customer cluster from Annual Income and Spending Score.')

    try:
        model, metadata = load_resources(MODEL_FILE, METADATA_FILE)
    except Exception as e:
        st.error(str(e))
        st.stop()

    feature_names = metadata['feature_names']
    simulated_data = np.array(metadata['simulated_data'])

    st.sidebar.header('Customer Input')
    annual_income = st.sidebar.slider(feature_names[0], 15, 150, 60)
    spending_score = st.sidebar.slider(feature_names[1], 1, 100, 50)

    customer_point = np.array([[annual_income, spending_score]])
    cluster_id = int(model.predict(customer_point)[0])

    left, right = st.columns([1, 2])

    with left:
        st.metric('Predicted Cluster ID', cluster_id)
        st.write('Selected Input')
        st.dataframe(
            {
                feature_names[0]: [annual_income],
                feature_names[1]: [spending_score],
            },
            use_container_width=True,
            hide_index=True,
        )

    with right:
        fig = build_scatter_plot(simulated_data, model, customer_point)
        st.pyplot(fig, use_container_width=True)


if __name__ == '__main__':
    main()
