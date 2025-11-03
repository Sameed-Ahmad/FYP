import google.generativeai as genai
from typing import List, Dict
import logging
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GeminiEmbeddings:
    """Handles text embeddings using Google Gemini"""
    
    def __init__(self):
        genai.configure(api_key=settings.google_api_key)
        # Using the latest text embedding model
        self.model_name = "models/text-embedding-004"
    
    def generate_embedding(self, text: str) -> List[float]:
        """
        Generate embedding for a single text
        
        Args:
            text: Input text to embed
            
        Returns:
            List of floats representing the embedding
        """
        try:
            result = genai.embed_content(
                model=self.model_name,
                content=text
            )
            return result['embedding']
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            raise
    
    def generate_embeddings_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embeddings
        """
        embeddings = []
        for text in texts:
            try:
                embedding = self.generate_embedding(text)
                embeddings.append(embedding)
            except Exception as e:
                logger.error(f"Failed to embed text: {text[:50]}... Error: {str(e)}")
                embeddings.append([])
        
        return embeddings
    
    def cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """
        Calculate cosine similarity between two vectors
        
        Args:
            vec1: First embedding vector
            vec2: Second embedding vector
            
        Returns:
            Cosine similarity score
        """
        if not vec1 or not vec2:
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = sum(a * a for a in vec1) ** 0.5
        magnitude2 = sum(b * b for b in vec2) ** 0.5
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def find_most_similar(
        self, 
        query: str, 
        candidates: List[str], 
        top_k: int = 5
    ) -> List[Dict[str, any]]:
        """
        Find most similar texts to query using embeddings
        
        Args:
            query: Query text
            candidates: List of candidate texts
            top_k: Number of top results to return
            
        Returns:
            List of dictionaries with text and similarity score
        """
        query_embedding = self.generate_embedding(query)
        candidate_embeddings = self.generate_embeddings_batch(candidates)
        
        similarities = []
        for i, candidate_emb in enumerate(candidate_embeddings):
            if candidate_emb:
                score = self.cosine_similarity(query_embedding, candidate_emb)
                similarities.append({
                    "text": candidates[i],
                    "score": score,
                    "index": i
                })
        
        # Sort by similarity score
        similarities.sort(key=lambda x: x["score"], reverse=True)
        
        return similarities[:top_k]