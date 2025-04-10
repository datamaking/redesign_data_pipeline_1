# 1. FACTORY DESIGN PATTERN FOR NLP PIPELINE

class TextProcessor:
    """Base text processor class"""
    
    def process(self, text):
        raise NotImplementedError("Subclasses must implement process method")


class BasicTextProcessor(TextProcessor):
    """Simple text processor implementation"""
    
    def process(self, text):
        # Basic cleaning and normalization
        processed = text.lower().strip()
        return processed


class AdvancedTextProcessor(TextProcessor):
    """Advanced text processor with more cleaning operations"""
    
    def process(self, text):
        # Advanced cleaning, normalization, and preprocessing
        import re
        processed = text.lower().strip()
        processed = re.sub(r'[^\w\s]', '', processed)  # Remove punctuation
        processed = re.sub(r'\s+', ' ', processed)     # Replace multiple spaces
        return processed


class NLPProcessorFactory:
    """Factory for creating text processors"""
    
    @staticmethod
    def get_processor(processor_type):
        if processor_type == "basic":
            return BasicTextProcessor()
        elif processor_type == "advanced":
            return AdvancedTextProcessor()
        else:
            raise ValueError(f"Unknown processor type: {processor_type}")


# Example usage of Factory pattern
def factory_example():
    # Client code
    factory = NLPProcessorFactory()
    
    # Get a basic processor
    basic_processor = factory.get_processor("basic")
    result1 = basic_processor.process("  HELLO World!  ")
    print(f"Basic processor result: '{result1}'")
    
    # Get an advanced processor
    advanced_processor = factory.get_processor("advanced")
    result2 = advanced_processor.process("  HELLO, World! How are you?  ")
    print(f"Advanced processor result: '{result2}'")


# 2. STRATEGY DESIGN PATTERN FOR NLP PIPELINE

class TextTokenizationStrategy:
    """Base strategy for text tokenization"""
    
    def tokenize(self, text):
        raise NotImplementedError("Subclasses must implement tokenize method")


class SimpleTokenizer(TextTokenizationStrategy):
    """Simple space-based tokenization"""
    
    def tokenize(self, text):
        return text.split()


class RegexTokenizer(TextTokenizationStrategy):
    """Regex-based tokenization"""
    
    def tokenize(self, text):
        import re
        return re.findall(r'\b\w+\b', text)


class NLTKTokenizer(TextTokenizationStrategy):
    """NLTK-based tokenization"""
    
    def tokenize(self, text):
        try:
            from nltk.tokenize import word_tokenize
            return word_tokenize(text)
        except ImportError:
            print("NLTK not installed, falling back to simple tokenization")
            return text.split()


class TextTokenizer:
    """Context class that uses a tokenization strategy"""
    
    def __init__(self, strategy=None):
        self.strategy = strategy if strategy else SimpleTokenizer()
    
    def set_strategy(self, strategy):
        self.strategy = strategy
    
    def tokenize_text(self, text):
        return self.strategy.tokenize(text)


# Example usage of Strategy pattern
def strategy_example():
    # Client code
    sample_text = "Hello, world! This is a test of the tokenization strategies."
    
    tokenizer = TextTokenizer()
    
    # Using default (simple) strategy
    tokens1 = tokenizer.tokenize_text(sample_text)
    print(f"Simple tokenizer: {tokens1}")
    
    # Change to regex strategy
    tokenizer.set_strategy(RegexTokenizer())
    tokens2 = tokenizer.tokenize_text(sample_text)
    print(f"Regex tokenizer: {tokens2}")
    
    # Change to NLTK strategy
    tokenizer.set_strategy(NLTKTokenizer())
    tokens3 = tokenizer.tokenize_text(sample_text)
    print(f"NLTK tokenizer: {tokens3}")


# 3. TEMPLATE DESIGN PATTERN FOR NLP PIPELINE

class NLPPipelineTemplate:
    """Template class defining the NLP pipeline structure"""
    
    def process_data(self, data):
        """Template method that defines the skeleton of the pipeline"""
        clean_data = self.clean(data)
        tokens = self.tokenize(clean_data)
        features = self.extract_features(tokens)
        transformed_data = self.transform(features)
        return self.load(transformed_data)
    
    def clean(self, data):
        """Clean and preprocess the raw text data"""
        raise NotImplementedError("Subclasses must implement clean method")
    
    def tokenize(self, clean_data):
        """Tokenize the cleaned text"""
        raise NotImplementedError("Subclasses must implement tokenize method")
    
    def extract_features(self, tokens):
        """Extract features from tokens"""
        raise NotImplementedError("Subclasses must implement extract_features method")
    
    def transform(self, features):
        """Transform the features into the desired format"""
        raise NotImplementedError("Subclasses must implement transform method")
    
    def load(self, transformed_data):
        """Load the transformed data to destination"""
        raise NotImplementedError("Subclasses must implement load method")


class SimpleSentimentPipeline(NLPPipelineTemplate):
    """Simple sentiment analysis pipeline implementation"""
    
    def clean(self, data):
        print("Cleaning text data...")
        return data.lower().strip()
    
    def tokenize(self, clean_data):
        print("Tokenizing cleaned data...")
        return clean_data.split()
    
    def extract_features(self, tokens):
        print("Extracting sentiment features...")
        # Simple sentiment lexicon
        positive_words = set(['good', 'great', 'excellent', 'happy', 'positive'])
        negative_words = set(['bad', 'terrible', 'awful', 'sad', 'negative'])
        
        pos_count = sum(1 for word in tokens if word in positive_words)
        neg_count = sum(1 for word in tokens if word in negative_words)
        
        return {'positive_count': pos_count, 'negative_count': neg_count}
    
    def transform(self, features):
        print("Transforming features to sentiment score...")
        sentiment_score = features['positive_count'] - features['negative_count']
        return {'sentiment_score': sentiment_score}
    
    def load(self, transformed_data):
        print("Loading results to destination...")
        return transformed_data


class NamedEntityPipeline(NLPPipelineTemplate):
    """Named entity recognition pipeline implementation"""
    
    def clean(self, data):
        print("Cleaning text data for NER...")
        return data.strip()
    
    def tokenize(self, clean_data):
        print("Tokenizing data for NER...")
        return clean_data.split()
    
    def extract_features(self, tokens):
        print("Extracting named entity features...")
        # Simple capitalization-based entity detection
        entities = [token for token in tokens if token[0].isupper()]
        return {'entities': entities}
    
    def transform(self, features):
        print("Transforming entity features...")
        # Group entities by presumed type (simplified)
        result = {'entities': features['entities']}
        return result
    
    def load(self, transformed_data):
        print("Loading NER results to destination...")
        return transformed_data


# Example usage of Template pattern
def template_example():
    # Client code
    text = "John loves New York. The city is beautiful and amazing!"
    
    # Use sentiment pipeline
    print("RUNNING SENTIMENT PIPELINE")
    sentiment_pipeline = SimpleSentimentPipeline()
    sentiment_result = sentiment_pipeline.process_data(text)
    print(f"Sentiment result: {sentiment_result}")
    
    print("\nRUNNING NER PIPELINE")
    # Use NER pipeline
    ner_pipeline = NamedEntityPipeline()
    ner_result = ner_pipeline.process_data(text)
    print(f"NER result: {ner_result}")


if __name__ == "__main__":
    print("===== FACTORY PATTERN EXAMPLE =====")
    factory_example()
    
    print("\n===== STRATEGY PATTERN EXAMPLE =====")
    strategy_example()
    
    print("\n===== TEMPLATE PATTERN EXAMPLE =====")
    template_example()




===========

=============

# Template Method Design Pattern for NLP ETL Pipeline

from abc import ABC, abstractmethod
import pandas as pd
import json
import os
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NLPPipeline(ABC):
    """Abstract base class implementing the template method pattern for NLP pipelines"""
    
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.data = None
        self.processed_data = None
    
    def run(self):
        """Template method that defines the algorithm skeleton"""
        start_time = time.time()
        logger.info(f"Starting {self.__class__.__name__} pipeline")
        
        try:
            # Extract phase
            logger.info("Extracting data")
            self.data = self.extract()
            logger.info(f"Extracted {len(self.data)} records")
            
            # Transform phases - hooks for customization
            logger.info("Preprocessing data")
            self.data = self.preprocess()
            
            logger.info("Applying NLP processing")
            self.processed_data = self.process_nlp()
            
            logger.info("Performing feature engineering")
            self.processed_data = self.engineer_features()
            
            # Custom transformation specific to pipeline
            logger.info("Applying custom transformation")
            self.processed_data = self.transform()
            
            # Load phase
            logger.info("Loading data to destination")
            self.load()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Pipeline completed successfully in {elapsed_time:.2f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self.handle_error(e)
            return False
    
    def extract(self):
        """Base implementation for data extraction"""
        extension = os.path.splitext(self.input_path)[1].lower()
        
        if extension == '.csv':
            return pd.read_csv(self.input_path)
        elif extension == '.json':
            with open(self.input_path, 'r') as f:
                return pd.DataFrame(json.load(f))
        elif extension == '.parquet':
            return pd.read_parquet(self.input_path)
        else:
            # Default case, override in subclass if needed
            raise ValueError(f"Unsupported file format: {extension}")
    
    @abstractmethod
    def preprocess(self):
        """Preprocess the data - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def process_nlp(self):
        """Apply NLP processing - must be implemented by subclasses"""
        pass
    
    def engineer_features(self):
        """Feature engineering - optional hook with default implementation"""
        # Default implementation just returns the data unchanged
        return self.processed_data
    
    @abstractmethod
    def transform(self):
        """Apply custom transformations - must be implemented by subclasses"""
        pass
    
    def load(self):
        """Base implementation for data loading"""
        extension = os.path.splitext(self.output_path)[1].lower()
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        
        if extension == '.csv':
            self.processed_data.to_csv(self.output_path, index=False)
        elif extension == '.json':
            with open(self.output_path, 'w') as f:
                json.dump(self.processed_data.to_dict(orient='records'), f, indent=2)
        elif extension == '.parquet':
            self.processed_data.to_parquet(self.output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {extension}")
    
    def handle_error(self, error):
        """Default error handling - can be overridden by subclasses"""
        logger.error(f"Error in pipeline: {str(error)}")
        # In a real implementation, you might want to:
        # 1. Send alerts
        # 2. Write to error logs
        # 3. Clean up temporary files
        # 4. Roll back partial changes


class SentimentAnalysisPipeline(NLPPipeline):
    """Concrete implementation of an NLP pipeline for sentiment analysis"""
    
    def __init__(self, input_path, output_path, text_column, stopwords=None):
        super().__init__(input_path, output_path)
        self.text_column = text_column
        self.stopwords = stopwords or []
    
    def preprocess(self):
        """Preprocess text data for sentiment analysis"""
        # Check if text column exists
        if self.text_column not in self.data.columns:
            raise ValueError(f"Text column '{self.text_column}' not found in data")
        
        # Remove rows with missing text
        self.data = self.data.dropna(subset=[self.text_column])
        
        # Basic text cleaning
        self.data['clean_text'] = self.data[self.text_column].str.lower()
        self.data['clean_text'] = self.data['clean_text'].str.replace(r'[^\w\s]', '', regex=True)
        
        # Remove stopwords
        if self.stopwords:
            self.data['clean_text'] = self.data['clean_text'].apply(
                lambda x: ' '.join([word for word in x.split() if word not in self.stopwords])
            )
        
        return self.data
    
    def process_nlp(self):
        """Apply sentiment analysis to the text"""
        # In a real implementation, you would use a sentiment analysis model here
        # This is a simple example using word counting as a proxy
        
        # Count positive and negative words
        positive_words = ['good', 'great', 'excellent', 'best', 'love', 'happy', 'positive']
        negative_words = ['bad', 'worst', 'terrible', 'hate', 'awful', 'negative']
        
        def simple_sentiment(text):
            text = text.lower()
            words = text.split()
            positive_count = sum(word in positive_words for word in words)
            negative_count = sum(word in negative_words for word in words)
            
            if positive_count > negative_count:
                return 'positive'
            elif negative_count > positive_count:
                return 'negative'
            else:
                return 'neutral'
        
        self.data['sentiment'] = self.data['clean_text'].apply(simple_sentiment)
        
        # Count sentiment words
        self.data['positive_count'] = self.data['clean_text'].apply(
            lambda x: sum(word in positive_words for word in x.split())
        )
        self.data['negative_count'] = self.data['clean_text'].apply(
            lambda x: sum(word in negative_words for word in x.split())
        )
        
        return self.data
    
    def engineer_features(self):
        """Create additional features for sentiment analysis"""
        # Calculate text length features
        self.data['char_count'] = self.data[self.text_column].str.len()
        self.data['word_count'] = self.data[self.text_column].str.split().str.len()
        
        # Calculate sentiment score
        self.data['sentiment_score'] = (self.data['positive_count'] - self.data['negative_count']) / \
                                      (self.data['positive_count'] + self.data['negative_count'] + 1)  # +1 to avoid division by zero
        
        return self.data
    
    def transform(self):
        """Apply final transformations specific to sentiment analysis"""
        # Select and rename columns for final output
        result = self.data[[self.text_column, 'clean_text', 'sentiment', 'sentiment_score', 
                           'positive_count', 'negative_count', 'char_count', 'word_count']]
        
        # Add a confidence column (simplified example)
        result['confidence'] = (result['positive_count'] + result['negative_count']) / result['word_count']
        result['confidence'] = result['confidence'].fillna(0)
        
        return result


class NamedEntityPipeline(NLPPipeline):
    """Concrete implementation of an NLP pipeline for named entity recognition"""
    
    def __init__(self, input_path, output_path, text_column, entities_to_extract=None):
        super().__init__(input_path, output_path)
        self.text_column = text_column
        self.entities_to_extract = entities_to_extract or ["PERSON", "ORG", "LOC", "DATE"]
    
    def preprocess(self):
        """Preprocess text data for NER"""
        # Check if text column exists
        if self.text_column not in self.data.columns:
            raise ValueError(f"Text column '{self.text_column}' not found in data")
        
        # Remove rows with missing text
        self.data = self.data.dropna(subset=[self.text_column])
        
        # Basic text cleaning, but preserve case for NER
        self.data['clean_text'] = self.data[self.text_column].str.replace(r'[^\w\s]', ' ', regex=True)
        self.data['clean_text'] = self.data['clean_text'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        return self.data
    
    def process_nlp(self):
        """Apply named entity recognition to the text"""
        # In a real implementation, you would use a proper NER model here
        # This is a simple example using basic pattern matching
        
        def extract_simple_entities(text):
            results = {entity_type: [] for entity_type in self.entities_to_extract}
            
            # Very simplified patterns for demonstration
            words = text.split()
            
            for word in words:
                # Check for capitalized words as potential entities
                if word and word[0].isupper() and len(word) > 1:
                    # Simplified logic - in a real case, use a proper NER model
                    if any(month in word for month in ["January", "February", "March", "April", "May", "June", 
                                                       "July", "August", "September", "October", "November", "December"]):
                        results["DATE"].append(word)
                    elif any(suffix in word for suffix in ["Inc", "Corp", "LLC", "Ltd"]):
                        results["ORG"].append(word)
                    elif any(place in word for place in ["New York", "London", "Paris", "Berlin"]):
                        results["LOC"].append(word)
                    else:
                        # Default to PERSON for other capitalized words
                        results["PERSON"].append(word)
            
            return results
        
        # Extract entities from each text
        self.data['entities'] = self.data['clean_text'].apply(extract_simple_entities)
        
        # Extract specific entity types as separate columns
        for entity_type in self.entities_to_extract:
            self.data[f'{entity_type.lower()}_entities'] = self.data['entities'].apply(
                lambda x: x.get(entity_type, [])
            )
        
        return self.data
    
    def engineer_features(self):
        """Create additional features for NER analysis"""
        # Count entities by type
        for entity_type in self.entities_to_extract:
            self.data[f'{entity_type.lower()}_count'] = self.data[f'{entity_type.lower()}_entities'].apply(len)
        
        # Total entity count
        self.data['total_entity_count'] = sum(
            self.data[f'{entity_type.lower()}_count'] for entity_type in self.entities_to_extract
        )
        
        # Entity density (entities per word)
        self.data['word_count'] = self.data['clean_text'].apply(lambda x: len(x.split()))
        self.data['entity_density'] = self.data['total_entity_count'] / self.data['word_count'].apply(
            lambda x: max(x, 1)  # Avoid division by zero
        )
        
        return self.data
    
    def transform(self):
        """Apply final transformations specific to NER"""
        # Create a flat view with one row per entity
        entities_list = []
        
        for idx, row in self.data.iterrows():
            doc_id = idx
            text = row[self.text_column]
            
            for entity_type in self.entities_to_extract:
                for entity in row[f'{entity_type.lower()}_entities']:
                    entities_list.append({
                        'doc_id': doc_id,
                        'text': text,
                        'entity': entity,
                        'entity_type': entity_type
                    })
        
        # Convert to DataFrame
        if entities_list:
            entities_df = pd.DataFrame(entities_list)
            # Combine with original data if needed
            result = entities_df
        else:
            # No entities found, return empty DataFrame with expected columns
            result = pd.DataFrame(columns=['doc_id', 'text', 'entity', 'entity_type'])
        
        return result


# Example usage
if __name__ == "__main__":
    # Create sample data
    sample_data = [
        {"id": 1, "text": "Apple Inc. is planning to open a new office in New York next January."},
        {"id": 2, "text": "The customer service was excellent, I love their products!"},
        {"id": 3, "text": "The movie was terrible, I hated every minute of it."},
        {"id": 4, "text": "Microsoft Corp and Google LLC announced a partnership yesterday."}
    ]
    
    # Save sample data to files
    os.makedirs("data", exist_ok=True)
    
    with open("data/sample.json", "w") as f:
        json.dump(sample_data, f)
    
    # Create and run sentiment analysis pipeline
    sentiment_pipeline = SentimentAnalysisPipeline(
        input_path="data/sample.json",
        output_path="data/sentiment_results.csv",
        text_column="text"
    )
    sentiment_pipeline.run()
    
    # Create and run named entity pipeline
    ner_pipeline = NamedEntityPipeline(
        input_path="data/sample.json",
        output_path="data/ner_results.csv",
        text_column="text"
    )
    ner_pipeline.run()
    
    # Display results
    print("\nSentiment Analysis Results:")
    sentiment_results = pd.read_csv("data/sentiment_results.csv")
    print(sentiment_results[["text", "sentiment", "sentiment_score"]])
    
    print("\nNamed Entity Recognition Results:")
    ner_results = pd.read_csv("data/ner_results.csv")
    print(ner_results)


========
==========

# Factory Design Pattern for NLP Processors

from abc import ABC, abstractmethod
import spacy
import nltk
from transformers import pipeline

class NLPProcessor(ABC):
    """Abstract base class for NLP processors"""
    
    @abstractmethod
    def process(self, text):
        """Process the input text"""
        pass
    
    @abstractmethod
    def get_name(self):
        """Return the name of the processor"""
        pass

class SpacyProcessor(NLPProcessor):
    """NLP processor using spaCy"""
    
    def __init__(self, model="en_core_web_sm"):
        self.nlp = spacy.load(model)
        self.model = model
    
    def process(self, text):
        doc = self.nlp(text)
        return {
            "tokens": [token.text for token in doc],
            "lemmas": [token.lemma_ for token in doc],
            "pos_tags": [token.pos_ for token in doc],
            "entities": [(ent.text, ent.label_) for ent in doc.ents]
        }
    
    def get_name(self):
        return f"SpaCy ({self.model})"

class NLTKProcessor(NLPProcessor):
    """NLP processor using NLTK"""
    
    def __init__(self):
        # Ensure required resources are downloaded
        nltk.download('punkt', quiet=True)
        nltk.download('wordnet', quiet=True)
        nltk.download('averaged_perceptron_tagger', quiet=True)
        nltk.download('maxent_ne_chunker', quiet=True)
        nltk.download('words', quiet=True)
        
        from nltk.stem import WordNetLemmatizer
        self.lemmatizer = WordNetLemmatizer()
    
    def process(self, text):
        tokens = nltk.word_tokenize(text)
        pos_tags = nltk.pos_tag(tokens)
        lemmas = [self.lemmatizer.lemmatize(token) for token in tokens]
        
        # Named entity recognition
        ne_tree = nltk.ne_chunk(pos_tags)
        entities = []
        for subtree in ne_tree:
            if hasattr(subtree, 'label'):
                entity_text = ' '.join([word for word, tag in subtree.leaves()])
                entity_type = subtree.label()
                entities.append((entity_text, entity_type))
        
        return {
            "tokens": tokens,
            "lemmas": lemmas,
            "pos_tags": [tag for _, tag in pos_tags],
            "entities": entities
        }
    
    def get_name(self):
        return "NLTK"

class HuggingFaceProcessor(NLPProcessor):
    """NLP processor using Hugging Face Transformers"""
    
    def __init__(self, model="distilbert-base-uncased"):
        self.ner_pipeline = pipeline("ner", model="dbmdz/bert-large-cased-finetuned-conll03-english")
        self.model = model
    
    def process(self, text):
        entities = self.ner_pipeline(text)
        
        # Group entities by word
        processed_entities = []
        current_entity = ""
        current_type = ""
        
        for entity in entities:
            if entity["entity"].startswith("B-"):
                if current_entity:
                    processed_entities.append((current_entity.strip(), current_type))
                current_entity = entity["word"]
                current_type = entity["entity"][2:]  # Remove B- prefix
            elif entity["entity"].startswith("I-"):
                current_entity += " " + entity["word"]
        
        if current_entity:
            processed_entities.append((current_entity.strip(), current_type))
        
        return {
            "entities": processed_entities
        }
    
    def get_name(self):
        return f"HuggingFace ({self.model})"

class NLPProcessorFactory:
    """Factory class for creating NLP processors"""
    
    @staticmethod
    def create_processor(processor_type, **kwargs):
        """
        Create and return an NLP processor based on the specified type
        
        Args:
            processor_type: Type of processor to create ('spacy', 'nltk', or 'huggingface')
            **kwargs: Additional arguments to pass to the processor constructor
        
        Returns:
            An instance of a concrete NLPProcessor
        """
        if processor_type.lower() == "spacy":
            return SpacyProcessor(**kwargs)
        elif processor_type.lower() == "nltk":
            return NLTKProcessor()
        elif processor_type.lower() == "huggingface":
            return HuggingFaceProcessor(**kwargs)
        else:
            raise ValueError(f"Unknown processor type: {processor_type}")


# Example usage
if __name__ == "__main__":
    # Create processors using the factory
    spacy_processor = NLPProcessorFactory.create_processor("spacy")
    nltk_processor = NLPProcessorFactory.create_processor("nltk")
    hf_processor = NLPProcessorFactory.create_processor("huggingface")
    
    # Process some text
    text = "Apple Inc. is planning to open a new office in New York next year."
    
    print(f"Processing with {spacy_processor.get_name()}:")
    spacy_results = spacy_processor.process(text)
    print(spacy_results)
    
    print(f"\nProcessing with {nltk_processor.get_name()}:")
    nltk_results = nltk_processor.process(text)
    print(nltk_results)
    
    print(f"\nProcessing with {hf_processor.get_name()}:")
    hf_results = hf_processor.process(text)
    print(hf_results)

===========

# Strategy Design Pattern for Text Preprocessing

from abc import ABC, abstractmethod
import re
import string
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer

# Download required NLTK resources
nltk.download('stopwords', quiet=True)
nltk.download('wordnet', quiet=True)

class TextPreprocessingStrategy(ABC):
    """Abstract base class for text preprocessing strategies"""
    
    @abstractmethod
    def preprocess(self, text):
        """Preprocess the input text"""
        pass
    
    @abstractmethod
    def get_description(self):
        """Return a description of the preprocessing strategy"""
        pass

class MinimalPreprocessingStrategy(TextPreprocessingStrategy):
    """Minimal preprocessing: just lowercase and remove extra whitespace"""
    
    def preprocess(self, text):
        # Convert to lowercase
        text = text.lower()
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def get_description(self):
        return "Minimal preprocessing (lowercase, whitespace normalization)"

class StandardPreprocessingStrategy(TextPreprocessingStrategy):
    """Standard preprocessing: lowercase, remove punctuation, and stopwords"""
    
    def __init__(self, language="english"):
        self.stop_words = set(stopwords.words(language))
        self.language = language
    
    def preprocess(self, text):
        # Convert to lowercase
        text = text.lower()
        
        # Remove punctuation
        text = text.translate(str.maketrans('', '', string.punctuation))
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove stopwords
        words = text.split()
        words = [word for word in words if word not in self.stop_words]
        
        return ' '.join(words)
    
    def get_description(self):
        return f"Standard preprocessing (lowercase, punctuation removal, {self.language} stopwords removal)"

class AdvancedPreprocessingStrategy(TextPreprocessingStrategy):
    """Advanced preprocessing: standard + stemming/lemmatization"""
    
    def __init__(self, use_stemming=False, language="english"):
        self.stop_words = set(stopwords.words(language))
        self.language = language
        self.use_stemming = use_stemming
        
        if use_stemming:
            self.stemmer = PorterStemmer()
        else:
            self.lemmatizer = WordNetLemmatizer()
    
    def preprocess(self, text):
        # Convert to lowercase
        text = text.lower()
        
        # Remove punctuation
        text = text.translate(str.maketrans('', '', string.punctuation))
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Tokenize
        words = text.split()
        
        # Remove stopwords and apply stemming/lemmatization
        filtered_words = []
        for word in words:
            if word not in self.stop_words:
                if self.use_stemming:
                    filtered_words.append(self.stemmer.stem(word))
                else:
                    filtered_words.append(self.lemmatizer.lemmatize(word))
        
        return ' '.join(filtered_words)
    
    def get_description(self):
        stem_lem = "stemming" if self.use_stemming else "lemmatization"
        return f"Advanced preprocessing (lowercase, punctuation removal, {self.language} stopwords removal, {stem_lem})"

class DomainSpecificPreprocessingStrategy(TextPreprocessingStrategy):
    """Domain-specific preprocessing for specialized text (e.g., medical, legal)"""
    
    def __init__(self, domain, custom_stopwords=None, replacements=None):
        self.domain = domain
        self.custom_stopwords = custom_stopwords or set()
        self.replacements = replacements or {}
        
        # Add standard stopwords
        self.stop_words = set(stopwords.words("english")).union(self.custom_stopwords)
    
    def preprocess(self, text):
        # Convert to lowercase
        text = text.lower()
        
        # Apply domain-specific replacements
        for original, replacement in self.replacements.items():
            text = text.replace(original, replacement)
        
        # Remove punctuation
        text = text.translate(str.maketrans('', '', string.punctuation))
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove stopwords
        words = text.split()
        words = [word for word in words if word not in self.stop_words]
        
        return ' '.join(words)
    
    def get_description(self):
        return f"Domain-specific preprocessing for {self.domain} text"

class TextPreprocessor:
    """Context class that uses a preprocessing strategy"""
    
    def __init__(self, strategy=None):
        self.strategy = strategy or MinimalPreprocessingStrategy()
    
    def set_strategy(self, strategy):
        """Change the preprocessing strategy at runtime"""
        self.strategy = strategy
    
    def preprocess(self, text):
        """Preprocess text using the current strategy"""
        return self.strategy.preprocess(text)
    
    def get_strategy_description(self):
        """Get a description of the current preprocessing strategy"""
        return self.strategy.get_description()


# Example usage
if __name__ == "__main__":
    # Sample text
    text = "The patient presented with severe abdominal pain and a fever of 102°F. Previous history of appendicitis."
    
    # Create preprocessor with default strategy (minimal)
    preprocessor = TextPreprocessor()
    
    print("Original text:")
    print(text)
    
    # Process with minimal strategy
    print("\nUsing strategy:", preprocessor.get_strategy_description())
    print(preprocessor.preprocess(text))
    
    # Change to standard strategy
    preprocessor.set_strategy(StandardPreprocessingStrategy())
    print("\nUsing strategy:", preprocessor.get_strategy_description())
    print(preprocessor.preprocess(text))
    
    # Change to advanced strategy with stemming
    preprocessor.set_strategy(AdvancedPreprocessingStrategy(use_stemming=True))
    print("\nUsing strategy:", preprocessor.get_strategy_description())
    print(preprocessor.preprocess(text))
    
    # Change to domain-specific strategy for medical text
    medical_replacements = {
        "°f": " degrees fahrenheit",
        "102°f": "102 degrees fahrenheit"
    }
    medical_stopwords = {"the", "with", "a", "of", "and"}
    preprocessor.set_strategy(DomainSpecificPreprocessingStrategy(
        domain="medical",
        custom_stopwords=medical_stopwords,
        replacements=medical_replacements
    ))
    print("\nUsing strategy:", preprocessor.get_strategy_description())
    print(preprocessor.preprocess(text))


========
==========

# Template Method Design Pattern for NLP ETL Pipeline

from abc import ABC, abstractmethod
import pandas as pd
import json
import os
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NLPPipeline(ABC):
    """Abstract base class implementing the template method pattern for NLP pipelines"""
    
    def __init__(self, input_path, output_path):
        self.input_path = input_path
        self.output_path = output_path
        self.data = None
        self.processed_data = None
    
    def run(self):
        """Template method that defines the algorithm skeleton"""
        start_time = time.time()
        logger.info(f"Starting {self.__class__.__name__} pipeline")
        
        try:
            # Extract phase
            logger.info("Extracting data")
            self.data = self.extract()
            logger.info(f"Extracted {len(self.data)} records")
            
            # Transform phases - hooks for customization
            logger.info("Preprocessing data")
            self.data = self.preprocess()
            
            logger.info("Applying NLP processing")
            self.processed_data = self.process_nlp()
            
            logger.info("Performing feature engineering")
            self.processed_data = self.engineer_features()
            
            # Custom transformation specific to pipeline
            logger.info("Applying custom transformation")
            self.processed_data = self.transform()
            
            # Load phase
            logger.info("Loading data to destination")
            self.load()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Pipeline completed successfully in {elapsed_time:.2f} seconds")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self.handle_error(e)
            return False
    
    def extract(self):
        """Base implementation for data extraction"""
        extension = os.path.splitext(self.input_path)[1].lower()
        
        if extension == '.csv':
            return pd.read_csv(self.input_path)
        elif extension == '.json':
            with open(self.input_path, 'r') as f:
                return pd.DataFrame(json.load(f))
        elif extension == '.parquet':
            return pd.read_parquet(self.input_path)
        else:
            # Default case, override in subclass if needed
            raise ValueError(f"Unsupported file format: {extension}")
    
    @abstractmethod
    def preprocess(self):
        """Preprocess the data - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def process_nlp(self):
        """Apply NLP processing - must be implemented by subclasses"""
        pass
    
    def engineer_features(self):
        """Feature engineering - optional hook with default implementation"""
        # Default implementation just returns the data unchanged
        return self.processed_data
    
    @abstractmethod
    def transform(self):
        """Apply custom transformations - must be implemented by subclasses"""
        pass
    
    def load(self):
        """Base implementation for data loading"""
        extension = os.path.splitext(self.output_path)[1].lower()
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        
        if extension == '.csv':
            self.processed_data.to_csv(self.output_path, index=False)
        elif extension == '.json':
            with open(self.output_path, 'w') as f:
                json.dump(self.processed_data.to_dict(orient='records'), f, indent=2)
        elif extension == '.parquet':
            self.processed_data.to_parquet(self.output_path, index=False)
        else:
            raise ValueError(f"Unsupported output format: {extension}")
    
    def handle_error(self, error):
        """Default error handling - can be overridden by subclasses"""
        logger.error(f"Error in pipeline: {str(error)}")
        # In a real implementation, you might want to:
        # 1. Send alerts
        # 2. Write to error logs
        # 3. Clean up temporary files
        # 4. Roll back partial changes


class SentimentAnalysisPipeline(NLPPipeline):
    """Concrete implementation of an NLP pipeline for sentiment analysis"""
    
    def __init__(self, input_path, output_path, text_column, stopwords=None):
        super().__init__(input_path, output_path)
        self.text_column = text_column
        self.stopwords = stopwords or []
    
    def preprocess(self):
        """Preprocess text data for sentiment analysis"""
        # Check if text column exists
        if self.text_column not in self.data.columns:
            raise ValueError(f"Text column '{self.text_column}' not found in data")
        
        # Remove rows with missing text
        self.data = self.data.dropna(subset=[self.text_column])
        
        # Basic text cleaning
        self.data['clean_text'] = self.data[self.text_column].str.lower()
        self.data['clean_text'] = self.data['clean_text'].str.replace(r'[^\w\s]', '', regex=True)
        
        # Remove stopwords
        if self.stopwords:
            self.data['clean_text'] = self.data['clean_text'].apply(
                lambda x: ' '.join([word for word in x.split() if word not in self.stopwords])
            )
        
        return self.data
    
    def process_nlp(self):
        """Apply sentiment analysis to the text"""
        # In a real implementation, you would use a sentiment analysis model here
        # This is a simple example using word counting as a proxy
        
        # Count positive and negative words
        positive_words = ['good', 'great', 'excellent', 'best', 'love', 'happy', 'positive']
        negative_words = ['bad', 'worst', 'terrible', 'hate', 'awful', 'negative']
        
        def simple_sentiment(text):
            text = text.lower()
            words = text.split()
            positive_count = sum(word in positive_words for word in words)
            negative_count = sum(word in negative_words for word in words)
            
            if positive_count > negative_count:
                return 'positive'
            elif negative_count > positive_count:
                return 'negative'
            else:
                return 'neutral'
        
        self.data['sentiment'] = self.data['clean_text'].apply(simple_sentiment)
        
        # Count sentiment words
        self.data['positive_count'] = self.data['clean_text'].apply(
            lambda x: sum(word in positive_words for word in x.split())
        )
        self.data['negative_count'] = self.data['clean_text'].apply(
            lambda x: sum(word in negative_words for word in x.split())
        )
        
        return self.data
    
    def engineer_features(self):
        """Create additional features for sentiment analysis"""
        # Calculate text length features
        self.data['char_count'] = self.data[self.text_column].str.len()
        self.data['word_count'] = self.data[self.text_column].str.split().str.len()
        
        # Calculate sentiment score
        self.data['sentiment_score'] = (self.data['positive_count'] - self.data['negative_count']) / \
                                      (self.data['positive_count'] + self.data['negative_count'] + 1)  # +1 to avoid division by zero
        
        return self.data
    
    def transform(self):
        """Apply final transformations specific to sentiment analysis"""
        # Select and rename columns for final output
        result = self.data[[self.text_column, 'clean_text', 'sentiment', 'sentiment_score', 
                           'positive_count', 'negative_count', 'char_count', 'word_count']]
        
        # Add a confidence column (simplified example)
        result['confidence'] = (result['positive_count'] + result['negative_count']) / result['word_count']
        result['confidence'] = result['confidence'].fillna(0)
        
        return result


class NamedEntityPipeline(NLPPipeline):
    """Concrete implementation of an NLP pipeline for named entity recognition"""
    
    def __init__(self, input_path, output_path, text_column, entities_to_extract=None):
        super().__init__(input_path, output_path)
        self.text_column = text_column
        self.entities_to_extract = entities_to_extract or ["PERSON", "ORG", "LOC", "DATE"]
    
    def preprocess(self):
        """Preprocess text data for NER"""
        # Check if text column exists
        if self.text_column not in self.data.columns:
            raise ValueError(f"Text column '{self.text_column}' not found in data")
        
        # Remove rows with missing text
        self.data = self.data.dropna(subset=[self.text_column])
        
        # Basic text cleaning, but preserve case for NER
        self.data['clean_text'] = self.data[self.text_column].str.replace(r'[^\w\s]', ' ', regex=True)
        self.data['clean_text'] = self.data['clean_text'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        return self.data
    
    def process_nlp(self):
        """Apply named entity recognition to the text"""
        # In a real implementation, you would use a proper NER model here
        # This is a simple example using basic pattern matching
        
        def extract_simple_entities(text):
            results = {entity_type: [] for entity_type in self.entities_to_extract}
            
            # Very simplified patterns for demonstration
            words = text.split()
            
            for word in words:
                # Check for capitalized words as potential entities
                if word and word[0].isupper() and len(word) > 1:
                    # Simplified logic - in a real case, use a proper NER model
                    if any(month in word for month in ["January", "February", "March", "April", "May", "June", 
                                                       "July", "August", "September", "October", "November", "December"]):
                        results["DATE"].append(word)
                    elif any(suffix in word for suffix in ["Inc", "Corp", "LLC", "Ltd"]):
                        results["ORG"].append(word)
                    elif any(place in word for place in ["New York", "London", "Paris", "Berlin"]):
                        results["LOC"].append(word)
                    else:
                        # Default to PERSON for other capitalized words
                        results["PERSON"].append(word)
            
            return results
        
        # Extract entities from each text
        self.data['entities'] = self.data['clean_text'].apply(extract_simple_entities)
        
        # Extract specific entity types as separate columns
        for entity_type in self.entities_to_extract:
            self.data[f'{entity_type.lower()}_entities'] = self.data['entities'].apply(
                lambda x: x.get(entity_type, [])
            )
        
        return self.data
    
    def engineer_features(self):
        """Create additional features for NER analysis"""
        # Count entities by type
        for entity_type in self.entities_to_extract:
            self.data[f'{entity_type.lower()}_count'] = self.data[f'{entity_type.lower()}_entities'].apply(len)
        
        # Total entity count
        self.data['total_entity_count'] = sum(
            self.data[f'{entity_type.lower()}_count'] for entity_type in self.entities_to_extract
        )
        
        # Entity density (entities per word)
        self.data['word_count'] = self.data['clean_text'].apply(lambda x: len(x.split()))
        self.data['entity_density'] = self.data['total_entity_count'] / self.data['word_count'].apply(
            lambda x: max(x, 1)  # Avoid division by zero
        )
        
        return self.data
    
    def transform(self):
        """Apply final transformations specific to NER"""
        # Create a flat view with one row per entity
        entities_list = []
        
        for idx, row in self.data.iterrows():
            doc_id = idx
            text = row[self.text_column]
            
            for entity_type in self.entities_to_extract:
                for entity in row[f'{entity_type.lower()}_entities']:
                    entities_list.append({
                        'doc_id': doc_id,
                        'text': text,
                        'entity': entity,
                        'entity_type': entity_type
                    })
        
        # Convert to DataFrame
        if entities_list:
            entities_df = pd.DataFrame(entities_list)
            # Combine with original data if needed
            result = entities_df
        else:
            # No entities found, return empty DataFrame with expected columns
            result = pd.DataFrame(columns=['doc_id', 'text', 'entity', 'entity_type'])
        
        return result


# Example usage
if __name__ == "__main__":
    # Create sample data
    sample_data = [
        {"id": 1, "text": "Apple Inc. is planning to open a new office in New York next January."},
        {"id": 2, "text": "The customer service was excellent, I love their products!"},
        {"id": 3, "text": "The movie was terrible, I hated every minute of it."},
        {"id": 4, "text": "Microsoft Corp and Google LLC announced a partnership yesterday."}
    ]
    
    # Save sample data to files
    os.makedirs("data", exist_ok=True)
    
    with open("data/sample.json", "w") as f:
        json.dump(sample_data, f)
    
    # Create and run sentiment analysis pipeline
    sentiment_pipeline = SentimentAnalysisPipeline(
        input_path="data/sample.json",
        output_path="data/sentiment_results.csv",
        text_column="text"
    )
    sentiment_pipeline.run()
    
    # Create and run named entity pipeline
    ner_pipeline = NamedEntityPipeline(
        input_path="data/sample.json",
        output_path="data/ner_results.csv",
        text_column="text"
    )
    ner_pipeline.run()
    
    # Display results
    print("\nSentiment Analysis Results:")
    sentiment_results = pd.read_csv("data/sentiment_results.csv")
    print(sentiment_results[["text", "sentiment", "sentiment_score"]])
    
    print("\nNamed Entity Recognition Results:")
    ner_results = pd.read_csv("data/ner_results.csv")
    print(ner_results)


00000000000000

0000000000


Design Patterns for NLP ETL Data Pipelines
I'll walk through three key design patterns (Factory, Strategy, and Template) and how they can be applied to NLP ETL data pipelines, with Python code examples for each.
Factory Design Pattern
The Factory pattern creates objects without exposing the instantiation logic and refers to the newly created object through a common interface.
Design Patterns for NLP ETL PipelinesCode 
When to Use the Factory Pattern in NLP Pipelines
The Factory pattern is ideal when:

Multiple NLP libraries or models need to be supported: You may need to switch between spaCy, NLTK, or Hugging Face models based on specific requirements.
Encapsulation of creation logic is needed: When the process of creating NLP processors involves complex initialization or configuration.
Loose coupling is desired: Your application shouldn't depend directly on specific NLP implementations.
Configuration flexibility: When you need to select different NLP processors at runtime based on configuration.

Step-by-Step Implementation:

Define an abstract base class (NLPProcessor) with methods that all processors must implement.
Create concrete classes for each NLP library/approach (SpaCy, NLTK, Hugging Face).
Implement a factory class (NLPProcessorFactory) that handles the creation logic.
Use the factory to create the appropriate processor based on requirements.

Strategy Design Pattern
The Strategy pattern defines a family of algorithms, encapsulates each one, and makes them interchangeable. It lets the algorithm vary independently from clients that use it.
Strategy Pattern for NLP ETL PipelineCode 
When to Use the Strategy Pattern in NLP Pipelines
The Strategy pattern is perfect for:

Interchangeable text preprocessing methods: When you need different text cleaning approaches based on data characteristics.
Varying algorithms for specific pipeline stages: Use different tokenization, normalization, or feature extraction algorithms.
Runtime flexibility: Change processing algorithms dynamically based on input data or processing needs.
Isolation of algorithm variations: Separate the algorithm implementation from its usage context.

Step-by-Step Implementation:

Define a strategy interface (TextPreprocessingStrategy) with a common method (preprocess).
Create concrete strategy classes (minimal, standard, advanced preprocessing).
Implement a context class (TextPreprocessor) that uses the strategy.
Allow the client to select or change strategies at runtime.

Template Method Design Pattern
The Template Method pattern defines the skeleton of an algorithm, deferring some steps to subclasses. It lets subclasses redefine certain steps without changing the algorithm's structure.
Template Method Pattern for NLP ETL PipelineCode 
When to Use the Template Method Pattern in NLP Pipelines
The Template Method pattern is ideal when:

Multiple pipelines share a common structure: When different NLP pipelines follow the same general ETL flow.
Clear separation of fixed and variable parts: The overall pipeline structure is fixed, but specific processing steps vary.
Code reuse and organization: Share common functionality while allowing specialization.
Enforcing a workflow: Ensure all pipeline implementations follow a consistent sequence of steps.

Step-by-Step Implementation:

Define an abstract base class (NLPPipeline) with a template method (run).
Implement invariant parts in the base class (data loading/saving, error handling).
Declare abstract methods for steps that must be implemented by concrete pipelines.
Create concrete pipeline implementations (sentiment analysis, named entity recognition).

Comparison and Decision Guide
Design PatternBest Used WhenBenefitsConsiderationsFactoryYou need to create different NLP processors/models based on varying requirements- Encapsulates object creation<br>- Allows runtime flexibility<br>- Promotes loose coupling- Adds complexity for simple scenarios<br>- May require additional interfacesStrategyYou need interchangeable algorithms for specific pipeline stages- Enables runtime algorithm switching2 / 2RetryClaude hit the max length for a message and has paused its response. You can write Continue to keep the chat going.Claude can make mistakes. Please double-check responses.