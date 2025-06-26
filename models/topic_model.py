from sentence_transformers import SentenceTransformer

class SentenceEmbeddingModel:
    """
    A class to generate embeddings for a list of texts using a specified SentenceTransformer model.
    """

    def __init__(self, model_name='all-MiniLM-L6-v2', device=None):
        """
        Initialize the SentenceEmbeddingModel.

        Args:
            model_name (str): Name of the pre-trained model to use.
            device (str or None): Device to run the model on ('cpu', 'cuda', or None for auto).
        """
        self.model = SentenceTransformer(model_name, device=device)

    def encode(self, texts, batch_size=32, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True):
        """
        Generate embeddings for a list of texts.

        Args:
            texts (List[str]): List of input texts.
            batch_size (int): Batch size for encoding.
            show_progress_bar (bool): Whether to show a progress bar.
            convert_to_numpy (bool): Whether to return numpy array.
            normalize_embeddings (bool): Whether to normalize embeddings.

        Returns:
            np.ndarray: Embeddings array.
        """
        embeddings = self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=show_progress_bar,
            convert_to_numpy=convert_to_numpy,
            normalize_embeddings=normalize_embeddings
        )
        return embeddings

# Example usage:
# model = SentenceEmbeddingModel()
# embeddings = model.encode(texts)
