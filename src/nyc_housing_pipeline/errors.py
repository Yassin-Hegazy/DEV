"""Pipeline errors. Every message says what went wrong and where."""


class PipelineError(Exception):
    """Base class for expected pipeline failures."""


class ConfigError(PipelineError):
    """The configuration file is missing or invalid."""


class SourceError(PipelineError):
    """A source file is missing, ambiguous, unreadable, or not faithfully parseable."""


class ValidationError(PipelineError):
    """Bronze output, or a published snapshot, failed validation."""
