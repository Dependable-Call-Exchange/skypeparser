#!/usr/bin/env python3
"""
Dependency Injection Framework

This module provides a simple dependency injection container for the Skype Parser project.
It centralizes service registration and resolution, making it easier to manage dependencies
and improve testability.
"""

import logging
from typing import Dict, Any, Type, Callable, TypeVar, Optional, cast, get_type_hints

# Set up logging
logger = logging.getLogger(__name__)

# Type variable for generic service types
T = TypeVar('T')


class ServiceProvider:
    """
    A simple dependency injection container that manages service registration and resolution.

    This container supports:
    - Singleton services (one instance shared across the application)
    - Transient services (new instance created each time)
    - Factory functions (custom instantiation logic)
    """

    def __init__(self):
        """Initialize the service provider with empty registrations."""
        self._singletons: Dict[Type, Any] = {}
        self._transients: Dict[Type, Type] = {}
        self._factories: Dict[Type, Callable[..., Any]] = {}

    def register_singleton(self, service_type: Type[T], instance: T) -> None:
        """
        Register a singleton service with a pre-created instance.

        Args:
            service_type: The type/interface to register
            instance: The instance to use for this service
        """
        self._singletons[service_type] = instance
        logger.debug(f"Registered singleton for {service_type.__name__}")

    def register_singleton_class(self, service_type: Type[T], implementation_type: Type[T]) -> None:
        """
        Register a singleton service with a class that will be instantiated on first use.

        Args:
            service_type: The type/interface to register
            implementation_type: The implementation class to instantiate
        """
        # Create a factory function that will instantiate the implementation
        def factory():
            instance = self._create_instance(implementation_type)
            # Replace the factory with the instance for future resolutions
            self._singletons[service_type] = instance
            return instance

        # Store the factory in singletons dict
        self._singletons[service_type] = factory
        logger.debug(f"Registered singleton class {implementation_type.__name__} for {service_type.__name__}")

    def register_transient(self, service_type: Type[T], implementation_type: Type[T]) -> None:
        """
        Register a transient service that will be instantiated each time it's resolved.

        Args:
            service_type: The type/interface to register
            implementation_type: The implementation class to instantiate
        """
        self._transients[service_type] = implementation_type
        logger.debug(f"Registered transient {implementation_type.__name__} for {service_type.__name__}")

    def register_factory(self, service_type: Type[T], factory: Callable[..., T]) -> None:
        """
        Register a factory function that will create the service when needed.

        Args:
            service_type: The type/interface to register
            factory: A function that creates an instance of the service
        """
        self._factories[service_type] = factory
        logger.debug(f"Registered factory for {service_type.__name__}")

    def get(self, service_type: Type[T]) -> T:
        """
        Resolve a service by its type.

        Args:
            service_type: The type/interface to resolve

        Returns:
            An instance of the requested service

        Raises:
            KeyError: If the service type is not registered
        """
        # Check singletons first
        if service_type in self._singletons:
            singleton = self._singletons[service_type]
            # If the singleton is a factory function, call it to create the instance
            if callable(singleton) and not isinstance(singleton, type):
                return singleton()
            return singleton

        # Check transients
        if service_type in self._transients:
            implementation_type = self._transients[service_type]
            return self._create_instance(implementation_type)

        # Check factories
        if service_type in self._factories:
            factory = self._factories[service_type]
            return factory()

        # Service not found
        raise KeyError(f"No registration found for {service_type.__name__}")

    def _create_instance(self, implementation_type: Type[T]) -> T:
        """
        Create an instance of a type, resolving constructor dependencies if possible.

        Args:
            implementation_type: The type to instantiate

        Returns:
            An instance of the specified type
        """
        try:
            # Get constructor parameter types
            type_hints = get_type_hints(implementation_type.__init__)
            # Remove return type
            if 'return' in type_hints:
                del type_hints['return']

            # Try to resolve dependencies
            kwargs = {}
            for param_name, param_type in type_hints.items():
                # Skip self parameter
                if param_name == 'self':
                    continue

                try:
                    # Try to resolve the dependency
                    kwargs[param_name] = self.get(param_type)
                except KeyError:
                    # If we can't resolve it, let the constructor handle it
                    # (it might have default values)
                    pass

            # Create the instance with resolved dependencies
            return implementation_type(**kwargs)
        except Exception as e:
            logger.error(f"Error creating instance of {implementation_type.__name__}: {e}")
            # Fall back to creating without dependencies
            return implementation_type()


# Global service provider instance
_global_provider = ServiceProvider()

def get_service_provider() -> ServiceProvider:
    """
    Get the global service provider instance.

    Returns:
        The global ServiceProvider instance
    """
    return _global_provider

def get_service(service_type: Type[T]) -> T:
    """
    Resolve a service from the global service provider.

    Args:
        service_type: The type/interface to resolve

    Returns:
        An instance of the requested service
    """
    return _global_provider.get(service_type)