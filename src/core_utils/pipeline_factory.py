#!/usr/bin/env python3
"""
Pipeline Factory module for the ETL pipeline.

This module provides a factory for building ETL pipeline components with proper
dependency injection and configuration management.
"""

import logging
import os
from typing import Any, Dict, Optional, Type, TypeVar, cast

from src.core_utils.di import ServiceProvider, get_service_provider
from src.logging.error_handling import ErrorContext, handle_errors
from src.core_utils.interfaces import (
    ContentExtractorProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    LoaderProtocol,
    MessageHandlerFactoryProtocol,
    TransformerProtocol,
    ValidationServiceProtocol,
)
from src.validation.schema_validation import validate_config

from src.db.context import ETLContext
from src.db.extractor import Extractor
from src.db.loader import Loader
from src.db.transformer import Transformer

# Set up logger
logger = logging.getLogger(__name__)

# Type variable for component types
T = TypeVar("T")


class PipelineFactory:
    """
    Factory for creating and configuring ETL pipeline components.

    This factory manages the creation and configuration of ETL components,
    ensuring they are properly initialized with the correct dependencies.
    It supports custom component registration and configuration validation.
    """

    def __init__(
        self, config: Dict[str, Any], service_provider: Optional[ServiceProvider] = None
    ):
        """
        Initialize the pipeline factory.

        Args:
            config: Configuration dictionary for the pipeline
            service_provider: Optional service provider for dependency injection
        """
        # Validate configuration
        with ErrorContext(component="PipelineFactory", operation="initialization"):
            self.config = validate_config(config, config_type="etl_config")

        # Initialize service provider
        self.service_provider = service_provider or get_service_provider()

        # Create context
        self.context = self._create_context(self.config)

        # Register default implementations
        self._register_default_services()

        logger.info("Pipeline factory initialized")

    def _create_context(self, config: Dict[str, Any]) -> ETLContext:
        """
        Create an ETL context from configuration.

        Args:
            config: Configuration dictionary

        Returns:
            Initialized ETL context
        """
        # Extract database configuration
        db_config = config.get("database", {})

        # Extract processing parameters
        output_dir = config.get("output", {}).get("directory", "output")
        memory_limit_mb = config.get("memory_limit_mb", 1024)
        parallel_processing = config.get("use_parallel_processing", True)
        chunk_size = config.get("chunk_size", 1000)
        batch_size = config.get("db_batch_size", 100)
        max_workers = config.get("max_workers")

        # Extract attachment handling parameters
        download_attachments = config.get("attachments", {}).get("download", False)
        attachments_dir = config.get("attachments", {}).get("directory")
        generate_thumbnails = config.get("attachments", {}).get(
            "generate_thumbnails", True
        )
        extract_metadata = config.get("attachments", {}).get("extract_metadata", True)

        # Create context
        context = ETLContext(
            db_config=db_config,
            output_dir=output_dir,
            memory_limit_mb=memory_limit_mb,
            parallel_processing=parallel_processing,
            chunk_size=chunk_size,
            batch_size=batch_size,
            max_workers=max_workers,
            download_attachments=download_attachments,
            attachments_dir=attachments_dir,
            generate_thumbnails=generate_thumbnails,
            extract_metadata=extract_metadata,
        )

        return context

    def _register_default_services(self) -> None:
        """Register default service implementations."""
        # Register core components if not already registered
        if not self.service_provider._singletons.get(ExtractorProtocol):
            self.register_component(ExtractorProtocol, Extractor(context=self.context))

        if not self.service_provider._singletons.get(TransformerProtocol):
            self.register_component(
                TransformerProtocol, Transformer(context=self.context)
            )

        if not self.service_provider._singletons.get(LoaderProtocol):
            self.register_component(LoaderProtocol, Loader(context=self.context))

    def register_component(
        self, component_type: Type[T], component_instance: T
    ) -> None:
        """
        Register a component implementation.

        Args:
            component_type: The component type/interface
            component_instance: The component implementation
        """
        self.service_provider.register_singleton(component_type, component_instance)
        logger.debug(f"Registered {component_type.__name__} implementation")

    @handle_errors(
        log_level="ERROR", default_message="Failed to create pipeline component"
    )
    def create_component(self, component_type: Type[T]) -> T:
        """
        Create a component of the specified type.

        Args:
            component_type: The type of component to create

        Returns:
            An instance of the requested component

        Raises:
            ValueError: If the component type is not registered
        """
        with ErrorContext(component_type=component_type.__name__):
            component = self.service_provider.get(component_type)
            return component

    def create_extractor(self) -> ExtractorProtocol:
        """
        Create an extractor component.

        Returns:
            Configured extractor instance
        """
        return self.create_component(ExtractorProtocol)

    def create_transformer(self) -> TransformerProtocol:
        """
        Create a transformer component.

        Returns:
            Configured transformer instance
        """
        return self.create_component(TransformerProtocol)

    def create_loader(self) -> LoaderProtocol:
        """
        Create a loader component.

        Returns:
            Configured loader instance
        """
        return self.create_component(LoaderProtocol)

    def get_context(self) -> ETLContext:
        """
        Get the ETL context.

        Returns:
            The ETL context
        """
        return self.context

    def create_pipeline(self, pipeline_class: Optional[Type] = None):
        """
        Create a complete ETL pipeline with all components.

        Args:
            pipeline_class: Optional pipeline implementation class

        Returns:
            Configured pipeline instance
        """
from src.db.modular_pipeline import ModularETLPipeline

        # Use specified pipeline class or default
        pipeline_cls = pipeline_class or ModularETLPipeline

        # Create pipeline with components
        extractor = self.create_extractor()
        transformer = self.create_transformer()
        loader = self.create_loader()

        # Create and return pipeline
        pipeline = pipeline_cls(
            context=self.context,
            extractor=extractor,
            transformer=transformer,
            loader=loader,
        )

        return pipeline
