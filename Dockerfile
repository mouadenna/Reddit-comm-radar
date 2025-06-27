FROM bitnami/spark:4.0.0

USER root

# Install additional Python packages
RUN pip install --no-cache-dir \
    sentence-transformers \
    torch \
    transformers \
    scikit-learn \
    nltk

USER 1001 