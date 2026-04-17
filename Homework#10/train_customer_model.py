import pickle
import numpy as np
from sklearn.cluster import KMeans

RANDOM_SEED = 42
NUM_POINTS = 200
N_CLUSTERS = 5
MODEL_FILE = 'customer_kmeans_model_abhinita.pkl'
METADATA_FILE = 'customer_metadata.pkl'


def main() -> None:
    rng = np.random.default_rng(RANDOM_SEED)

    annual_income = rng.integers(15, 151, size=NUM_POINTS)
    spending_score = rng.integers(1, 101, size=NUM_POINTS)
    X = np.column_stack((annual_income, spending_score))

    model = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_SEED, n_init=10)
    model.fit(X)

    metadata = {
        'feature_names': ['Annual Income (k$)', 'Spending Score (1-100)'],
        'cluster_centers': model.cluster_centers_,
        'simulated_data': X,
        'random_seed': RANDOM_SEED,
        'num_points': NUM_POINTS,
    }

    with open(MODEL_FILE, 'wb') as f:
        pickle.dump(model, f)

    with open(METADATA_FILE, 'wb') as f:
        pickle.dump(metadata, f)

    print(f'Model saved to {MODEL_FILE}')
    print(f'Metadata saved to {METADATA_FILE}')
    print('Cluster centers:')
    print(model.cluster_centers_)


if __name__ == '__main__':
    main()
