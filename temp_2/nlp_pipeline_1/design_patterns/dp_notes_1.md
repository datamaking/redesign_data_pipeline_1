Let's explore the implementation of three design patterns (Factory, Strategy, Template) for an NLP ETL pipeline with code examples and detailed explanations:

### 1. Factory Pattern
**When to Use**: When you need to create different types of objects based on input/configuration without exposing creation logic
```python
from abc import ABC, abstractmethod

# Abstract Data Processor
class DataProcessor(ABC):
    @abstractmethod
    def extract(self):
        pass
    
    @abstractmethod
    def transform(self, text: str) -> str:
        pass
    
    @abstractmethod
    def load(self, data):
        pass

# Concrete Implementations
class CSVProcessor(DataProcessor):
    def extract(self):
        print("Extracting data from CSV")
        return "raw csv text"
    
    def transform(self, text):
        print("Transforming CSV data")
        return text.lower()
    
    def load(self, data):
        print(f"Loading CSV data: {data}")

class JSONProcessor(DataProcessor):
    def extract(self):
        print("Extracting data from JSON")
        return "raw json text"
    
    def transform(self, text):
        print("Transforming JSON data")
        return text.upper()
    
    def load(self, data):
        print(f"Loading JSON data: {data}")

# Factory Class
class DataProcessorFactory:
    @staticmethod
    def create_processor(file_type: str) -> DataProcessor:
        if file_type == "csv":
            return CSVProcessor()
        elif file_type == "json":
            return JSONProcessor()
        raise ValueError("Unsupported file type")

# Usage
processor = DataProcessorFactory.create_processor("csv")
raw_data = processor.extract()
processed_data = processor.transform(raw_data)
processor.load(processed_data)
```

### 2. Strategy Pattern
**When to Use**: When you need to switch between different algorithms/rules at runtime
```python
from abc import ABC, abstractmethod

# Cleaning Strategy Interface
class CleaningStrategy(ABC):
    @abstractmethod
    def clean(self, text: str) -> str:
        pass

# Concrete Strategies
class BasicCleaning(CleaningStrategy):
    def clean(self, text):
        print("Basic cleaning")
        return text.strip().lower()

class AdvancedCleaning(CleaningStrategy):
    def clean(self, text):
        print("Advanced cleaning")
        return text.strip().lower().replace("'", "")

# Context Class
class TextCleaner:
    def __init__(self, strategy: CleaningStrategy):
        self._strategy = strategy
    
    def set_strategy(self, strategy: CleaningStrategy):
        self._strategy = strategy
    
    def execute_cleaning(self, text: str) -> str:
        return self._strategy.clean(text)

# Usage
cleaner = TextCleaner(BasicCleaning())
result = cleaner.execute_cleaning(" Some Dirty TEXT ")
cleaner.set_strategy(AdvancedCleaning())
result = cleaner.execute_cleaning(" Another Dirty TEXT ")
```

### 3. Template Pattern
**When to Use**: When you want to define a common process with customizable steps
```python
from abc import ABC, abstractmethod

# Template Class
class PipelineTemplate(ABC):
    def execute_pipeline(self):
        data = self.extract()
        processed_data = self.transform(data)
        self.load(processed_data)
    
    @abstractmethod
    def extract(self):
        pass
    
    @abstractmethod
    def transform(self, data):
        pass
    
    @abstractmethod
    def load(self, data):
        pass

# Concrete Implementation
class SentimentAnalysisPipeline(PipelineTemplate):
    def extract(self):
        print("Extracting data from social media")
        return "Sample text for analysis"
    
    def transform(self, data):
        print("Performing sentiment analysis")
        return {"text": data, "sentiment": "positive"}
    
    def load(self, data):
        print(f"Storing results in database: {data}")

# Usage
pipeline = SentimentAnalysisPipeline()
pipeline.execute_pipeline()
```

### When to Use Which Pattern:

1. **Factory Pattern**:
- Use when creating different types of data processors based on input format
- When you need to centralize object creation logic
- When adding new data sources (e.g., XML, database) without changing client code
- *Example*: Switching between CSV/JSON processing based on file type

2. **Strategy Pattern**:
- Use when you need multiple variations of text cleaning/normalization
- When different NLP tasks require different preprocessing
- When you want to dynamically change processing rules at runtime
- *Example*: Switching between basic/advanced cleaning based on data quality

3. **Template Pattern**:
- Use when you have a fixed pipeline structure with customizable steps
- When enforcing a standard ETL process across different NLP tasks
- When creating base pipeline structure for different implementations
- *Example*: Maintaining consistent pipeline flow for different NLP tasks (sentiment analysis vs topic modeling)

### Step-by-Step Implementation Guide:

**Factory Pattern**:
1. Define abstract interface for data processors
2. Create concrete implementations for different formats
3. Implement factory class with creation method
4. Client code requests processor from factory

**Strategy Pattern**:
1. Define strategy interface for cleaning operations
2. Implement concrete strategy classes
3. Create context class that uses the strategy
4. Client selects and changes strategies at runtime

**Template Pattern**:
1. Create abstract base class with template method
2. Define abstract operations (extract/transform/load)
3. Implement concrete subclass with specific operations
4. Client executes template method which calls subclass implementations

These patterns help create:
- Maintainable pipeline architecture
- Easily extendable components
- Decoupled processing logic
- Reusable code structure

The choice depends on your specific needs:
- Object creation complexity → Factory
- Algorithm variations → Strategy
- Process structure → Template
