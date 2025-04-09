"""
HTML file data source implementation.
"""
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
import logging
import os
from bs4 import BeautifulSoup
from .base_source import BaseDataSource
from config.source_configs.html_config import HtmlSourceConfig

class HtmlDataSource(BaseDataSource):
    """Data source for reading from HTML files."""
    
    def __init__(self, spark: SparkSession, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the HTML file data source.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary for the HTML file source (optional)
        """
        if config is None:
            config = HtmlSourceConfig().get('html', {})
        super().__init__(spark, config)
        
    def read(self, path: Optional[str] = None, recursive: Optional[bool] = None, 
             encoding: Optional[str] = None, parser: Optional[str] = None, **kwargs) -> DataFrame:
        """
        Read data from HTML files.
        
        Args:
            path: Path pattern for HTML files (overrides config)
            recursive: Whether to recursively search for files (overrides config)
            encoding: Character encoding (overrides config)
            parser: HTML parser to use (overrides config)
            **kwargs: Additional read parameters
            
        Returns:
            DataFrame: PySpark DataFrame containing the HTML data
        """
        try:
            # Get parameters from config if not provided
            file_path = path or self.config.get('path', 'data/input/*.html')
            is_recursive = recursive if recursive is not None else self.config.get('recursive', True)
            file_encoding = encoding or self.config.get('encoding', 'utf-8')
            html_parser = parser or self.config.get('parser', 'html.parser')
            include_path = self.config.get('include_path', True)
            include_filename = self.config.get('include_filename', True)
            extract_text = self.config.get('extract_text', True)
            extract_title = self.config.get('extract_title', True)
            extract_metadata = self.config.get('extract_metadata', True)
            extract_links = self.config.get('extract_links', False)
            extract_images = self.config.get('extract_images', False)
            
            # Set up read options
            read_options = {
                'wholetext': 'true',
                'encoding': file_encoding,
                'recursiveFileLookup': str(is_recursive).lower()
            }
            
            # Add any additional options from kwargs
            for key, value in kwargs.items():
                if key not in ['path', 'recursive', 'encoding', 'parser']:
                    read_options[key] = value
                    
            # Read the HTML files as text
            self.logger.info(f"Reading HTML files from {file_path}")
            df = self.spark.read.format('text').options(**read_options).load(file_path)
            
            # Add file metadata if requested
            if include_path or include_filename:
                df = df.withColumn('input_file', self.spark.sql.functions.input_file_name())
                
                if include_filename:
                    df = df.withColumn('filename', self.spark.sql.functions.regexp_extract('input_file', r'[^/\\\\]+$', 0))
                    
                if not include_path:
                    df = df.drop('input_file')
            
            # Parse HTML content
            if extract_text or extract_title or extract_metadata or extract_links or extract_images:
                # Define schema for parsed HTML
                schema_fields = []
                if extract_text:
                    schema_fields.append(StructField("text", StringType(), True))
                if extract_title:
                    schema_fields.append(StructField("title", StringType(), True))
                if extract_links:
                    schema_fields.append(StructField("links", ArrayType(StringType()), True))
                if extract_images:
                    schema_fields.append(StructField("images", ArrayType(StringType()), True))
                if extract_metadata:
                    schema_fields.append(StructField("meta_description", StringType(), True))
                    schema_fields.append(StructField("meta_keywords", StringType(), True))
                
                parse_schema = StructType(schema_fields)
                
                # Define UDF for HTML parsing
                @udf(parse_schema)
                def parse_html(html_content):
                    if html_content is None:
                        return None
                        
                    try:
                        soup = BeautifulSoup(html_content, html_parser)
                        result = {}
                        
                        if extract_text:
                            # Remove script and style elements
                            for script in soup(["script", "style"]):
                                script.extract()
                            # Get text
                            text = soup.get_text(separator=' ', strip=True)
                            result["text"] = text
                            
                        if extract_title:
                            title_tag = soup.find('title')
                            result["title"] = title_tag.get_text() if title_tag else None
                            
                        if extract_links:
                            links = [a.get('href') for a in soup.find_all('a', href=True)]
                            result["links"] = links
                            
                        if extract_images:
                            images = [img.get('src') for img in soup.find_all('img', src=True)]
                            result["images"] = images
                            
                        if extract_metadata:
                            meta_desc = soup.find('meta', attrs={'name': 'description'})
                            result["meta_description"] = meta_desc.get('content') if meta_desc else None
                            
                            meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
                            result["meta_keywords"] = meta_keywords.get('content') if meta_keywords else None
                            
                        return result
                    except Exception as e:
                        # Return None for all fields on error
                        return None
                
                # Apply the parsing UDF
                df = df.withColumn("parsed_html", parse_html(col("value")))
                
                # Extract fields from the parsed HTML
                if extract_text:
                    df = df.withColumn("text", col("parsed_html.text"))
                if extract_title:
                    df = df.withColumn("title", col("parsed_html.title"))
                if extract_links:
                    df = df.withColumn("links", col("parsed_html.links"))
                if extract_images:
                    df = df.withColumn("images", col("parsed_html.images"))
                if extract_metadata:
                    df = df.withColumn("meta_description", col("parsed_html.meta_description"))
                    df = df.withColumn("meta_keywords", col("parsed_html.meta_keywords"))
                
                # Drop the intermediate parsed_html column
                df = df.drop("parsed_html")
            
            # Log the operation
            row_count = df.count()
            self._log_read_operation(f"HTML files at {file_path}", row_count)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading HTML files from {path or self.config.get('path', 'data/input/*.html')}: {str(e)}")
            raise
            
    def validate(self) -> bool:
        """
        Validate the HTML file source configuration.
        
        Returns:
            bool: True if validation is successful, False otherwise
        """
        try:
            # Check if the path exists
            file_path = self.config.get('path', 'data/input/*.html')
            
            # For glob patterns, check if the directory exists
            import glob
            if '*' in file_path or '?' in file_path:
                base_dir = os.path.dirname(file_path)
                if not os.path.exists(base_dir):
                    self.logger.warning(f"Base directory {base_dir} does not exist")
                    return False
                    
                # Check if any files match the pattern
                matching_files = glob.glob(file_path, recursive=self.config.get('recursive', True))
                if not matching_files:
                    self.logger.warning(f"No files match the pattern {file_path}")
                    return False
            else:
                # For direct file paths, check if the file exists
                if not os.path.exists(file_path):
                    self.logger.warning(f"File {file_path} does not exist")
                    return False
                    
            # Validate BeautifulSoup parser
            parser = self.config.get('parser', 'html.parser')
            valid_parsers = ['html.parser', 'lxml', 'html5lib', 'lxml-xml']
            if parser not in valid_parsers:
                self.logger.warning(f"Invalid HTML parser: {parser}. Valid options are: {', '.join(valid_parsers)}")
                return False
                
            self.logger.info("HTML file source validated successfully")
            return True
        except Exception as e:
            self.logger.error(f"HTML file source validation failed: {str(e)}")
            return False