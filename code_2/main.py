"""
Main entry point for the PySpark NLP pipeline.
"""
import os
import argparse
import yaml
import logging
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession

from config.base_config import BaseConfig
from src.utils.logging_utils import PipelineLogger
from src.utils.exception_utils import exception_handler, PipelineException

from src.data_source.hive_source import HiveDataSource
from src.data_source.text_source import TextDataSource
from src.data_source.html_source import HtmlDataSource
from src.data_source.rdbms_source import RdbmsDataSource
from src.data_source.mongodb_source import MongoDBDataSource
from src.data_source.chromadb_source import ChromaDBDataSource
from src.data_source.postgres_source import PostgresDataSource
from src.data_source.neo4j_source import Neo4jDataSource

from src.preprocessing.preprocessor import TextPreprocessor

from src.chunking.fixed_size_chunker import FixedSizeChunker
from src.chunking.sentence_chunker import SentenceChunker
from src.chunking.paragraph_chunker import ParagraphChunker
from src.chunking.semantic_chunker import SemanticChunker

from src.embedding.huggingface_embedder import HuggingFaceEmbedder
from src.embedding.spark_embedder import SparkEmbedder

from src.data_target.hive_target import HiveDataTarget
from src.data_target.file_target import FileDataTarget
from src.data_target.rdbms_target import RdbmsDataTarget
from src.data_target.mongodb_target import MongoDBDataTarget
from src.data_target.chromadb_target import ChromaDBDataTarget
from src.data_target.postgres_target import PostgresDataTarget
from src.data_target.neo4j_target import Neo4jDataTarget

from src.vector_search.chromadb_search import ChromaDBVectorSearch
from src.vector_search.postgres_search import PostgresVectorSearch
from src.vector_search.neo4j_search import Neo4jVectorSearch

class NLPPipeline:
    """Main NLP pipeline class that orchestrates the entire process."""
    
    def __init__(self, config_path: str):
        """
        Initialize the NLP pipeline.
        
        Args:
            config_path: Path to the configuration file
        """
        # Load configuration
        self.config = BaseConfig(config_path)
        
        # Initialize logging
        log_config = self.config.get('logging', {})
        self.logger_manager = PipelineLogger(log_config)
        self.logger = self.logger_manager.get_logger(self.__class__.__name__)
        
        # Initialize Spark session
        self.spark = self._create_spark_session()
        
        # Component factories
        self.source_factories = {
            'hive': HiveDataSource,
            'text': TextDataSource,
            'html': HtmlDataSource,
            'rdbms': RdbmsDataSource,
            'mongodb': MongoDBDataSource,
            'chromadb': ChromaDBDataSource,
            'postgres': PostgresDataSource,
            'neo4j': Neo4jDataSource
        }
        
        self.chunker_factories = {
            'fixed_size': FixedSizeChunker,
            'sentence': SentenceChunker,
            'paragraph': ParagraphChunker,
            'semantic': SemanticChunker
        }
        
        self.embedder_factories = {
            'huggingface': HuggingFaceEmbedder,
            'spark': SparkEmbedder
        }
        
        self.target_factories = {
            'hive': HiveDataTarget,
            'file': FileDataTarget,
            'rdbms': RdbmsDataTarget,
            'mongodb': MongoDBDataTarget,
            'chromadb': ChromaDBDataTarget,
            'postgres': PostgresDataTarget,
            'neo4j': Neo4jDataTarget
        }
        
        self.search_factories = {
            'chromadb': ChromaDBVectorSearch,
            'postgres': PostgresVectorSearch,
            'neo4j': Neo4jVectorSearch
        }
        
        self.logger.info("NLP pipeline initialized")
        
    def _create_spark_session(self) -> SparkSession:
        """
        Create a Spark session.
        
        Returns:
            SparkSession: Configured Spark session
        """
        spark_config = self.config.get('spark', {})
        app_name = spark_config.get('app_name', 'NLP Pipeline')
        
        # Create the Spark session builder
        builder = SparkSession.builder.appName(app_name)
        
        # Apply Spark configurations
        for key, value in spark_config.get('conf', {}).items():
            builder = builder.config(key, value)
            
        # Create and return the Spark session
        spark = builder.getOrCreate()
        self.logger.info(f"Created Spark session with app name: {app_name}")
        return spark
        
    @exception_handler()
    def run(self) -> None:
        """Run the complete NLP pipeline."""
        self.logger.info("Starting NLP pipeline")
        
        # Get pipeline configuration
        pipeline_config = self.config.get('pipeline', {})
        
        # 1. Read data from source
        source_data = self._read_from_source(pipeline_config.get('source', {}))
        
        # 2. Preprocess the data
        preprocessed_data = self._preprocess_data(source_data, pipeline_config.get('preprocessing', {}))
        
        # 3. Chunk the data
        chunked_data = self._chunk_data(preprocessed_data, pipeline_config.get('chunking', {}))
        
        # 4. Generate embeddings
        embedded_data = self._generate_embeddings(chunked_data, pipeline_config.get('embedding', {}))
        
        # 5. Write data to target
        self._write_to_target(embedded_data, pipeline_config.get('target', {}))
        
        # 6. Perform vector search (if configured)
        if 'search' in pipeline_config:
            self._perform_search(pipeline_config.get('search', {}))
            
        self.logger.info("NLP pipeline completed successfully")
        
    def _read_from_source(self, source_config: Dict[str, Any]):
        """Read data from the configured source."""
        source_type = source_config.get('type')
        if not source_type:
            raise PipelineException("Source type not specified in configuration")
            
        if source_type not in self.source_factories:
            raise PipelineException(f"Unsupported source type: {source_type}")
            
        self.logger.info(f"Reading data from source: {source_type}")
        source = self.source_factories[source_type](self.spark, source_config)
        
        # Validate the source
        if not source.validate():
            raise PipelineException(f"Source validation failed for {source_type}")
            
        # Read the data
        return source.read(**source_config.get('params', {}))
        
    def _preprocess_data(self, df, preprocessing_config: Dict[str, Any]):
        """Preprocess the data."""
        self.logger.info("Preprocessing data")
        preprocessor = TextPreprocessor(self.spark, preprocessing_config)
        return preprocessor.preprocess(
            df, 
            preprocessing_config.get('text_column', 'text'),
            preprocessing_config.get('output_column')
        )
        
    def _chunk_data(self, df, chunking_config: Dict[str, Any]):
        """Chunk the data."""
        chunker_type = chunking_config.get('type', 'fixed_size')
        if chunker_type not in self.chunker_factories:
            raise PipelineException(f"Unsupported chunker type: {chunker_type}")
            
        self.logger.info(f"Chunking data using: {chunker_type}")
        chunker = self.chunker_factories[chunker_type](self.spark, chunking_config)
        
        return chunker.chunk(
            df,
            chunking_config.get('text_column', 'text_processed'),
            chunking_config.get('id_column')
        )
        
    def _generate_embeddings(self, df, embedding_config: Dict[str, Any]):
        """Generate embeddings for the data."""
        embedder_type = embedding_config.get('type', 'huggingface')
        if embedder_type not in self.embedder_factories:
            raise PipelineException(f"Unsupported embedder type: {embedder_type}")
            
        self.logger.info(f"Generating embeddings using: {embedder_type}")
        embedder = self.embedder_factories[embedder_type](self.spark, embedding_config)
        
        return embedder.generate_embeddings(
            df,
            embedding_config.get('text_column', 'chunk_text'),
            embedding_config.get('output_column', 'embeddings')
        )
        
    def _write_to_target(self, df, target_config: Dict[str, Any]):
        """Write data to the configured target."""
        target_type = target_config.get('type')
        if not target_type:
            raise PipelineException("Target type not specified in configuration")
            
        if target_type not in self.target_factories:
            raise PipelineException(f"Unsupported target type: {target_type}")
            
        self.logger.info(f"Writing data to target: {target_type}")
        target = self.target_factories[target_type](self.spark, target_config)
        
        # Validate the target
        if not target.validate():
            raise PipelineException(f"Target validation failed for {target_type}")
            
        # Write the data
        target.write(df, **target_config.get('params', {}))
        
    def _perform_search(self, search_config: Dict[str, Any]):
        """Perform vector search."""
        search_type = search_config.get('type')
        if not search_type:
            raise PipelineException("Search type not specified in configuration")
            
        if search_type not in self.search_factories:
            raise PipelineException(f"Unsupported search type: {search_type}")
            
        self.logger.info(f"Performing vector search using: {search_type}")
        search = self.search_factories[search_type](self.spark, search_config)
        
        # Validate the search
        if not search.validate():
            raise PipelineException(f"Search validation failed for {search_type}")
            
        # Perform the search (this is just a validation, actual search would be done in an application)
        query_vector = search_config.get('query_vector')
        if query_vector:
            results = search.search(
                query_vector,
                search_config.get('top_k', 10)
            )
            self.logger.info(f"Found {len(results)} search results")
        
    def stop(self):
        """Stop the pipeline and clean up resources."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Stopped Spark session")

@exception_handler(exit_on_error=True)
def main():
    """Main entry point for the pipeline."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='PySpark NLP Pipeline')
    parser.add_argument('--config', type=str, required=True, help='Path to configuration file')
    args = parser.parse_args()
    
    # Run the pipeline
    pipeline = NLPPipeline(args.config)
    try:
        pipeline.run()
    finally:
        pipeline.stop()

if __name__ == '__main__':
    main()