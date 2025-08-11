"""
// Copyright (c) William Newport
// SPDX-License-Identifier: BUSL-1.1

Logging utilities for Yellow Data Platform optimized for Kubernetes environments.
Provides structured JSON logging with context tracking and performance monitoring.
"""

import json
import logging
import os
import time
from typing import Dict, Any, Optional, Union
from datetime import datetime, timezone
from contextlib import contextmanager
from contextvars import ContextVar
import traceback


# Context variables for request/operation tracking
request_id: ContextVar[Optional[str]] = ContextVar('request_id', default=None)
workspace_name: ContextVar[Optional[str]] = ContextVar('workspace_name', default=None)
platform_name: ContextVar[Optional[str]] = ContextVar('platform_name', default=None)
operation_name: ContextVar[Optional[str]] = ContextVar('operation_name', default=None)
psp_name: ContextVar[Optional[str]] = ContextVar('psp_name', default=None)


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging in Kubernetes environments"""

    def format(self, record: logging.LogRecord) -> str:
        # Base log entry structure
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat() + "Z",  # ISO format with UTC
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.thread,
            "process": record.process
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields if present (from extra parameter in log calls)
        if hasattr(record, 'extra_fields') and getattr(record, 'extra_fields', None):
            log_entry.update(getattr(record, 'extra_fields'))

        # Add context variables
        context = self._get_context_variables()
        if context:
            log_entry["context"] = context

        # Add Kubernetes-specific fields from environment
        k8s_fields = self._get_kubernetes_context()
        if k8s_fields:
            log_entry["kubernetes"] = k8s_fields

        return json.dumps(log_entry, default=str)  # default=str handles datetime objects

    def _get_context_variables(self) -> Dict[str, Any]:
        """Extract context from context variables"""
        context = {}

        if request_id.get():
            context['request_id'] = request_id.get()
        if workspace_name.get():
            context['workspace_name'] = workspace_name.get()
        if platform_name.get():
            context['platform_name'] = platform_name.get()
        if operation_name.get():
            context['operation_name'] = operation_name.get()
        if psp_name.get():
            context['psp_name'] = psp_name.get()
        return context

    def _get_kubernetes_context(self) -> Dict[str, Any]:
        """Extract Kubernetes context from environment variables"""
        k8s_context = {}

        # Common Kubernetes environment variables
        if pod_name := os.getenv('HOSTNAME'):  # In K8s, HOSTNAME is usually the pod name
            k8s_context['pod_name'] = pod_name

        if namespace := os.getenv('NAMESPACE'):
            k8s_context['namespace'] = namespace

        if node_name := os.getenv('NODE_NAME'):
            k8s_context['node_name'] = node_name

        if service_account := os.getenv('SERVICE_ACCOUNT'):
            k8s_context['service_account'] = service_account

        # Airflow-specific context
        if dag_id := os.getenv('AIRFLOW_CTX_DAG_ID'):
            k8s_context['dag_id'] = dag_id

        if task_id := os.getenv('AIRFLOW_CTX_TASK_ID'):
            k8s_context['task_id'] = task_id

        if execution_date := os.getenv('AIRFLOW_CTX_EXECUTION_DATE'):
            k8s_context['execution_date'] = execution_date

        # Yellow Platform specific environment variables
        if yellow_env := os.getenv('YELLOW_ENVIRONMENT'):
            k8s_context['yellow_environment'] = yellow_env

        if job_type := os.getenv('YELLOW_JOB_TYPE'):
            k8s_context['job_type'] = job_type

        return k8s_context


class ContextualLogger:
    """Logger that automatically includes context information and provides convenience methods"""

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)

    def _get_extra_fields(self, **kwargs: Any) -> Dict[str, Any]:
        """Combine context variables with additional fields"""
        extra_fields = {}

        # Add context variables
        if request_id.get():
            extra_fields['request_id'] = request_id.get()
        if workspace_name.get():
            extra_fields['workspace_name'] = workspace_name.get()
        if platform_name.get():
            extra_fields['platform_name'] = platform_name.get()
        if operation_name.get():
            extra_fields['operation_name'] = operation_name.get()
        if psp_name.get():
            extra_fields['psp_name'] = psp_name.get()
        # Add additional fields
        extra_fields.update(kwargs)

        return extra_fields

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message with context"""
        extra_fields = self._get_extra_fields(**kwargs)
        self.logger.debug(message, extra={'extra_fields': extra_fields})

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message with context"""
        extra_fields = self._get_extra_fields(**kwargs)
        self.logger.info(message, extra={'extra_fields': extra_fields})

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message with context"""
        extra_fields = self._get_extra_fields(**kwargs)
        self.logger.warning(message, extra={'extra_fields': extra_fields})

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message with context"""
        extra_fields = self._get_extra_fields(**kwargs)
        self.logger.error(message, extra={'extra_fields': extra_fields})

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message with context"""
        extra_fields = self._get_extra_fields(**kwargs)
        self.logger.critical(message, extra={'extra_fields': extra_fields})

    def exception(self, message: str, exc_info: Optional[Exception] = None, **kwargs: Any) -> None:
        """Log exception with full context and stack trace"""
        extra_fields = self._get_extra_fields(**kwargs)

        if exc_info:
            extra_fields['error_type'] = type(exc_info).__name__
            extra_fields['stack_trace'] = traceback.format_exc()

        self.logger.error(
            message,
            extra={'extra_fields': extra_fields},
            exc_info=exc_info is not None
        )


def setup_kubernetes_logging(level: str = "INFO", enable_json: bool = True) -> logging.Logger:
    """
    Setup logging optimized for Kubernetes environments

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        enable_json: Whether to use JSON formatting (True for production, False for development)

    Returns:
        Configured logger
    """
    # Get or create root logger
    logger = logging.getLogger()

    # Set log level
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Create console handler (stdout for Kubernetes)
    handler = logging.StreamHandler()

    if enable_json:
        # Use structured JSON formatter for production/Kubernetes
        formatter = StructuredFormatter()
    else:
        # Use simple formatter for development (similar to existing format)
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent propagation to avoid duplicate logs
    logger.propagate = False

    return logger


def get_logging_config_from_environment() -> Dict[str, Any]:
    """Get logging configuration from environment variables"""
    return {
        'level': os.getenv('LOG_LEVEL', 'INFO').upper(),
        'format': os.getenv('LOG_FORMAT', 'json').lower(),  # 'json' or 'text'
        'enable_json': os.getenv('LOG_FORMAT', 'json').lower() == 'json'
    }


def setup_logging_for_environment() -> logging.Logger:
    """Setup logging based on environment configuration"""
    config = get_logging_config_from_environment()
    return setup_kubernetes_logging(
        level=config['level'],
        enable_json=config['enable_json']
    )


def get_contextual_logger(name: str) -> ContextualLogger:
    """Get a contextual logger for the given name"""
    return ContextualLogger(name)


@contextmanager
def log_operation_timing(
    logger: Union[logging.Logger, ContextualLogger],
    operation_name_param: str,
    **context: Any
):
    """
    Context manager for timing operations and logging performance

    Args:
        logger: Logger instance to use
        operation_name_param: Name of the operation being timed
        **context: Additional context to include in logs
    """
    start_time = time.time()

    # Set operation context
    token = operation_name.set(operation_name_param)

    try:
        if isinstance(logger, ContextualLogger):
            logger.info(
                f"Starting operation: {operation_name_param}",
                event_type='operation_start',
                operation_name=operation_name_param,
                **context
            )
        else:
            logger.info(
                f"Starting operation: {operation_name_param}",
                extra={'extra_fields': {
                    'event_type': 'operation_start',
                    'operation_name': operation_name_param,
                    **context
                }}
            )

        yield

        duration = time.time() - start_time

        if isinstance(logger, ContextualLogger):
            logger.info(
                f"Operation {operation_name_param} completed successfully",
                event_type='operation_complete',
                operation_name=operation_name_param,
                duration_seconds=round(duration, 3),
                status='success',
                **context
            )
        else:
            logger.info(
                f"Operation {operation_name_param} completed successfully",
                extra={'extra_fields': {
                    'event_type': 'operation_complete',
                    'operation_name': operation_name_param,
                    'duration_seconds': round(duration, 3),
                    'status': 'success',
                    **context
                }}
            )

    except Exception as e:
        duration = time.time() - start_time

        if isinstance(logger, ContextualLogger):
            logger.exception(
                f"Operation {operation_name_param} failed",
                exc_info=e,
                event_type='operation_failed',
                operation_name=operation_name_param,
                duration_seconds=round(duration, 3),
                status='failed',
                error_message=str(e),
                **context
            )
        else:
            logger.error(
                f"Operation {operation_name_param} failed",
                extra={'extra_fields': {
                    'event_type': 'operation_failed',
                    'operation_name': operation_name_param,
                    'duration_seconds': round(duration, 3),
                    'status': 'failed',
                    'error_message': str(e),
                    'error_type': type(e).__name__,
                    **context
                }},
                exc_info=True
            )
        raise
    finally:
        # Reset operation context
        operation_name.reset(token)


def set_context(
    workspace: Optional[str] = None,
    psp: Optional[str] = None,
    platform: Optional[str] = None,
    request: Optional[str] = None
) -> None:
    """
    Set logging context variables

    Args:
        workspace: Workspace name
        platform: Platform name
        request: Request ID
    """
    if workspace is not None:
        workspace_name.set(workspace)
    if psp is not None:
        psp_name.set(psp)
    if platform is not None:
        platform_name.set(platform)
    if request is not None:
        request_id.set(request)


def log_startup_info(
    logger: Union[logging.Logger, ContextualLogger],
    component: str,
    version: Optional[str] = None,
    **context: Any
) -> None:
    """Log structured startup information"""
    startup_data = {
        'event_type': 'startup',
        'component': component,
        'timestamp': datetime.utcnow().isoformat() + "Z"
    }

    if version:
        startup_data['version'] = version

    startup_data.update(context)

    if isinstance(logger, ContextualLogger):
        logger.info(f"{component} starting up", **startup_data)
    else:
        logger.info(
            f"{component} starting up",
            extra={'extra_fields': startup_data}
        )


def log_health_check(
    logger: Union[logging.Logger, ContextualLogger],
    component: str,
    status: str,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """Log health check information"""
    health_data = {
        'event_type': 'health_check',
        'component': component,
        'status': status,
        'timestamp': datetime.utcnow().isoformat() + "Z"
    }

    if details:
        health_data.update(details)

    if isinstance(logger, ContextualLogger):
        logger.info(f"Health check for {component}: {status}", **health_data)
    else:
        logger.info(
            f"Health check for {component}: {status}",
            extra={'extra_fields': health_data}
        )


# Convenience functions for backward compatibility with existing logging patterns
def log_error(message: str, **context: Any) -> None:
    """Log error with level prefix for pod manager visibility (backward compatibility)"""
    logger = logging.getLogger(__name__)
    logger.error(f"[ERROR] {message}", extra={'extra_fields': context})


def log_warning(message: str, **context: Any) -> None:
    """Log warning with level prefix for pod manager visibility (backward compatibility)"""
    logger = logging.getLogger(__name__)
    logger.warning(f"[WARNING] {message}", extra={'extra_fields': context})


def log_info(message: str, **context: Any) -> None:
    """Log info with level prefix for pod manager visibility (backward compatibility)"""
    logger = logging.getLogger(__name__)
    logger.info(f"[INFO] {message}", extra={'extra_fields': context})
