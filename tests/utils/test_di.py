#!/usr/bin/env python3
"""
Tests for the dependency injection framework.
"""

import pytest
from typing import Protocol, Dict, Any

from src.utils.di import ServiceProvider, get_service_provider, get_service


# Define test protocols
class TestServiceProtocol(Protocol):
    def get_value(self) -> str:
        ...


class TestDependencyProtocol(Protocol):
    def get_name(self) -> str:
        ...


# Define test implementations
class TestDependency:
    def __init__(self, name: str = "TestDependency"):
        self.name = name

    def get_name(self) -> str:
        return self.name


class TestService:
    def __init__(self, dependency: TestDependencyProtocol = None):
        self.dependency = dependency or TestDependency()

    def get_value(self) -> str:
        return f"TestService with {self.dependency.get_name()}"


class AnotherTestService:
    def __init__(self, dependency: TestDependencyProtocol = None):
        self.dependency = dependency or TestDependency("AnotherDependency")

    def get_value(self) -> str:
        return f"AnotherTestService with {self.dependency.get_name()}"


# Tests
def test_service_provider_singleton():
    """Test that singleton services return the same instance."""
    provider = ServiceProvider()

    # Register a singleton
    service = TestService()
    provider.register_singleton(TestServiceProtocol, service)

    # Get the service twice
    service1 = provider.get(TestServiceProtocol)
    service2 = provider.get(TestServiceProtocol)

    # Check that they are the same instance
    assert service1 is service2
    assert service1 is service


def test_service_provider_singleton_class():
    """Test that singleton classes are instantiated once."""
    provider = ServiceProvider()

    # Register a singleton class
    provider.register_singleton_class(TestServiceProtocol, TestService)

    # Get the service twice
    service1 = provider.get(TestServiceProtocol)
    service2 = provider.get(TestServiceProtocol)

    # Check that they are the same instance
    assert service1 is service2
    assert isinstance(service1, TestService)


def test_service_provider_transient_class():
    """Test that transient classes are instantiated each time."""
    provider = ServiceProvider()

    # Register a transient
    provider.register_transient(TestServiceProtocol, TestService)

    # Get the service twice
    service1 = provider.get(TestServiceProtocol)
    service2 = provider.get(TestServiceProtocol)

    # Check that they are different instances
    assert service1 is not service2
    assert isinstance(service1, TestService)
    assert isinstance(service2, TestService)


def test_service_provider_factory():
    """Test that factory functions are called each time."""
    provider = ServiceProvider()

    # Register a factory function
    counter = [0]

    def factory():
        counter[0] += 1
        return TestService(TestDependency(f"Factory{counter[0]}"))

    provider.register_factory(TestServiceProtocol, factory)

    # Get the service twice
    service1 = provider.get(TestServiceProtocol)
    service2 = provider.get(TestServiceProtocol)

    # Check that they are different instances with different dependencies
    assert service1 is not service2
    assert service1.get_value() == "TestService with Factory1"
    assert service2.get_value() == "TestService with Factory2"


def test_service_provider_dependency_resolution():
    """Test that dependencies are resolved automatically."""
    provider = ServiceProvider()

    # Register dependencies
    provider.register_singleton_class(TestDependencyProtocol, TestDependency)
    provider.register_singleton_class(TestServiceProtocol, TestService)

    # Get the service
    service = provider.get(TestServiceProtocol)

    # Check that the dependency was injected
    assert isinstance(service.dependency, TestDependency)
    assert service.get_value() == "TestService with TestDependency"


def test_service_provider_missing_service():
    """Test that getting a missing service raises an exception."""
    provider = ServiceProvider()

    # Try to get a service that hasn't been registered
    with pytest.raises(KeyError):
        provider.get(TestServiceProtocol)


def test_global_service_provider():
    """Test the global service provider functions."""
    # Get the global provider
    provider = get_service_provider()

    # Register a service
    provider.register_singleton_class(TestServiceProtocol, TestService)

    # Get the service using the global function
    service = get_service(TestServiceProtocol)

    # Check that it's the right type
    assert isinstance(service, TestService)

    # Clear the provider for other tests
    provider._singletons.clear()
    provider._transients.clear()
    provider._factories.clear()


def test_service_override():
    """Test that services can be overridden."""
    provider = ServiceProvider()

    # Register a service
    provider.register_singleton_class(TestServiceProtocol, TestService)

    # Get the service
    service1 = provider.get(TestServiceProtocol)
    assert isinstance(service1, TestService)

    # Override the service
    provider.register_singleton_class(TestServiceProtocol, AnotherTestService)

    # Get the service again
    service2 = provider.get(TestServiceProtocol)
    assert isinstance(service2, AnotherTestService)

    # Check that they are different instances
    assert service1 is not service2