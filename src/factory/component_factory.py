from typing import Dict, Any, Type
from src.common.base import BaseExtractor, BaseTransformer, BaseLoader

class ComponentFactory:
    def __init__(self):
        self._components: Dict[str, Type] = {}

    def register(self, type_name: str, component_class: Type) -> None:
        """Register a new component"""
        self._components[type_name] = component_class

    def create(self, type_name: str, options: Dict[str, Any]):
        """Create a component instance"""
        if type_name not in self._components:
            raise ValueError(f"Unknown component type: {type_name}")
        return self._components[type_name](options)

class ExtractorFactory(ComponentFactory):
    """Factory for creating extractors"""
    pass

class TransformerFactory(ComponentFactory):
    """Factory for creating transformers"""
    pass

class LoaderFactory(ComponentFactory):
    """Factory for creating loaders"""
    pass

# Global factory instances
extractor_factory = ExtractorFactory()
transformer_factory = TransformerFactory()
loader_factory = LoaderFactory() 