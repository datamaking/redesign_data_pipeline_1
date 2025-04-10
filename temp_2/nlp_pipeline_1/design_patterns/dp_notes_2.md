Below, I will generate Python code for the **Factory Design Pattern**, **Strategy Design Pattern**, and **Template Method Design Pattern** tailored to building an NLP ETL (Extract, Transform, Load) data pipeline. I’ll provide a step-by-step explanation for each pattern, explain when to use it, and ensure the response is complete and clear. The context is an NLP ETL pipeline, where we extract text data from various sources, transform it using NLP techniques (e.g., tokenization, stemming), and load it into a destination like a database.

---

### 1. Factory Design Pattern

#### When to Use
The Factory Design Pattern is ideal when you need to create objects without specifying their exact classes upfront, delegating object creation to a factory class based on some input or configuration. In an NLP ETL pipeline, use this pattern when:
- You have multiple ways to extract data (e.g., from a database, API, or file) and want to instantiate the appropriate extractor dynamically.
- You need to decouple object creation from the client code, making it easy to add new extractor types without modifying the core pipeline logic.

#### Step-by-Step Explanation
1. **Define an Abstract Extractor Interface**: Create an abstract base class `Extractor` with an abstract `extract` method to enforce a common interface for all extractors.
2. **Implement Concrete Extractors**: Create specific extractor classes (e.g., `DatabaseExtractor`, `APIExtractor`, `FileExtractor`) that inherit from `Extractor` and implement the `extract` method.
3. **Create a Factory Class**: Define an `ExtractorFactory` with a method `get_extractor` that takes a source type as input and returns the appropriate extractor instance.
4. **Use the Factory in the Pipeline**: The client code requests an extractor from the factory based on the source type and uses it to extract data.

#### Python Code
```python
from abc import ABC, abstractmethod

# Step 1: Abstract Extractor Interface
class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass

# Step 2: Concrete Extractor Classes
class DatabaseExtractor(Extractor):
    def extract(self):
        print("Extracting text data from database")
        return "Text data from database"

class APIExtractor(Extractor):
    def extract(self):
        print("Extracting text data from API")
        return "Text data from API"

class FileExtractor(Extractor):
    def extract(self):
        print("Extracting text data from file")
        return "Text data from file"

# Step 3: Factory Class
class ExtractorFactory:
    def get_extractor(self, source_type):
        if source_type == "database":
            return DatabaseExtractor()
        elif source_type == "api":
            return APIExtractor()
        elif source_type == "file":
            return FileExtractor()
        else:
            raise ValueError(f"Unknown source type: {source_type}")

# Step 4: Usage in the Pipeline
def main():
    factory = ExtractorFactory()
    source_type = "database"  # Could come from config
    extractor = factory.get_extractor(source_type)
    data = extractor.extract()
    print(f"Extracted data: {data}")

if __name__ == "__main__":
    main()
```

#### Output
```
Extracting text data from database
Extracted data: Text data from database
```

#### Why Factory Here?
In an NLP ETL pipeline, data sources vary (databases, APIs, files), and the Factory pattern allows you to encapsulate the creation logic. If a new source (e.g., web scraper) is added, you only update the factory, not the pipeline itself.

---

### 2. Strategy Design Pattern

#### When to Use
The Strategy Design Pattern is perfect when you have multiple algorithms or strategies for a task, and you want to select one at runtime without hardcoding the choice. In an NLP ETL pipeline, use this pattern when:
- The transformation step has multiple possible approaches (e.g., different tokenization methods or processing pipelines like sentiment analysis vs. entity recognition).
- You need flexibility to swap algorithms dynamically based on requirements, such as choosing between a simple whitespace tokenizer and an advanced NLTK tokenizer.

#### Step-by-Step Explanation
1. **Define a Strategy Interface**: Create an abstract `Tokenizer` class with an abstract `tokenize` method to define the contract for tokenization strategies.
2. **Implement Concrete Strategies**: Create specific tokenizer classes (e.g., `WhitespaceTokenizer`, `NLTKTokenizer`) that implement the `tokenize` method.
3. **Create a Transformer Class**: Define a `Transformer` class that accepts a tokenizer strategy via its constructor and uses it to process data.
4. **Configure and Use the Transformer**: Pass the desired strategy to the transformer and apply it to the data.

#### Python Code
```python
from abc import ABC, abstractmethod

# Step 1: Strategy Interface
class Tokenizer(ABC):
    @abstractmethod
    def tokenize(self, text):
        pass

# Step 2: Concrete Strategy Classes
class WhitespaceTokenizer(Tokenizer):
    def tokenize(self, text):
        print("Tokenizing with whitespace splitter")
        return text.split()

class NLTKTokenizer(Tokenizer):
    def tokenize(self, text):
        print("Tokenizing with NLTK")
        # Requires NLTK: pip install nltk
        # import nltk; nltk.download('punkt')
        from nltk.tokenize import word_tokenize
        return word_tokenize(text)

# Step 3: Transformer Class with Strategy
class Transformer:
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer

    def transform(self, data):
        tokens = self.tokenizer.tokenize(data)
        # Additional NLP steps could be added here (e.g., stemming)
        return tokens

# Step 4: Usage in the Pipeline
def main():
    data = "Hello world, this is NLP!"
    
    # Use Whitespace Tokenizer
    whitespace_tokenizer = WhitespaceTokenizer()
    transformer1 = Transformer(whitespace_tokenizer)
    result1 = transformer1.transform(data)
    print(f"Whitespace tokens: {result1}")
    
    # Use NLTK Tokenizer
    nltk_tokenizer = NLTKTokenizer()
    transformer2 = Transformer(nltk_tokenizer)
    result2 = transformer2.transform(data)
    print(f"NLTK tokens: {result2}")

if __name__ == "__main__":
    main()
```

#### Output
```
Tokenizing with whitespace splitter
Whitespace tokens: ['Hello', 'world,', 'this', 'is', 'NLP!']
Tokenizing with NLTK
NLTK tokens: ['Hello', 'world', ',', 'this', 'is', 'NLP', '!']
```

#### Why Strategy Here?
In NLP, transformation steps like tokenization can vary (whitespace splitting vs. sophisticated tokenizers). The Strategy pattern allows you to plug in different algorithms at runtime, enhancing flexibility and reusability in the transform phase.

---

### 3. Template Method Design Pattern

#### When to Use
The Template Method Design Pattern is suitable when you have an algorithm with a fixed structure but variable steps, allowing subclasses to customize those steps. In an NLP ETL pipeline, use this pattern when:
- The ETL process has a consistent sequence (extract, transform, load), but the implementation of each step differs based on the use case (e.g., sentiment analysis vs. entity recognition).
- You want to enforce the pipeline structure while allowing specific implementations for different NLP tasks.

#### Step-by-Step Explanation
1. **Define an Abstract ETL Base Class**: Create an abstract `ETL` class with a `run` method that outlines the pipeline sequence (extract, transform, load) and abstract methods for each step.
2. **Implement Concrete ETL Classes**: Create specific ETL classes (e.g., `SentimentETL`, `EntityETL`) that inherit from `ETL` and provide implementations for the abstract methods.
3. **Execute the Pipeline**: Instantiate a concrete ETL class and call `run` to execute the predefined sequence with customized steps.

#### Python Code
```python
from abc import ABC, abstractmethod

# Step 1: Abstract ETL Base Class
class ETL(ABC):
    def run(self):
        """Template method defining the ETL pipeline structure"""
        data = self.extract()
        transformed_data = self.transform(data)
        self.load(transformed_data)

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self, data):
        pass

    @abstractmethod
    def load(self, data):
        pass

# Step 2: Concrete ETL Classes
class SentimentETL(ETL):
    def extract(self):
        print("Extracting data for sentiment analysis")
        return "Raw text for sentiment"

    def transform(self, data):
        print("Transforming data for sentiment analysis")
        # Simple transformation: lowercase for consistency
        return data.lower()

    def load(self, data):
        print("Loading sentiment-transformed data into database")

class EntityETL(ETL):
    def extract(self):
        print("Extracting data for entity recognition")
        return "Raw text for entities"

    def transform(self, data):
        print("Transforming data for entity recognition")
        # Simple transformation: uppercase to simulate entity tagging prep
        return data.upper()

    def load(self, data):
        print("Loading entity-transformed data into database")

# Step 3: Usage in the Pipeline
def main():
    print("Running Sentiment ETL:")
    sentiment_etl = SentimentETL()
    sentiment_etl.run()
    
    print("\nRunning Entity ETL:")
    entity_etl = EntityETL()
    entity_etl.run()

if __name__ == "__main__":
    main()
```

#### Output
```
Running Sentiment ETL:
Extracting data for sentiment analysis
Transforming data for sentiment analysis
Loading sentiment-transformed data into database

Running Entity ETL:
Extracting data for entity recognition
Transforming data for entity recognition
Loading entity-transformed data into database
```

#### Why Template Method Here?
The ETL process has a fixed sequence (extract, transform, load), but the details of each step depend on the NLP task (e.g., sentiment analysis vs. entity recognition). The Template Method pattern ensures the structure remains consistent while allowing customization.

---

### Summary: When to Use Each Pattern in an NLP ETL Pipeline

- **Factory Design Pattern**:
  - **Use Case**: Creating extractors, transformers, or loaders based on configuration (e.g., source type: database, API, file).
  - **Why**: Encapsulates object creation, making it easy to extend with new types without altering the pipeline.
  - **Example**: `ExtractorFactory` to instantiate extractors for different data sources.

- **Strategy Design Pattern**:
  - **Use Case**: Selecting different algorithms within the transform step (e.g., tokenization methods, stemming algorithms).
  - **Why**: Provides runtime flexibility to swap strategies, enhancing modularity in NLP processing.
  - **Example**: `Transformer` with interchangeable `Tokenizer` strategies.

- **Template Method Design Pattern**:
  - **Use Case**: Defining the overall ETL process with a fixed sequence but variable step implementations for different NLP tasks.
  - **Why**: Enforces a consistent pipeline structure while allowing specific behaviors to be defined in subclasses.
  - **Example**: `ETL` base class with `run` method, implemented by `SentimentETL` and `EntityETL`.

---

### Optional Integration Note
While these patterns can be used independently, they can also be combined in a real-world NLP ETL pipeline:
- Use **Factory** to create components (extractors, transformers, loaders).
- Use **Strategy** within the transformer for flexible NLP processing.
- Use **Template Method** (or a composition variant) to define the pipeline sequence.

For simplicity, the examples above are standalone, but a production pipeline might compose these patterns for maximum flexibility and maintainability.