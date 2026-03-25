"""Structured errors for pipeline input validation (API-friendly)."""


class PipelineInputError(ValueError):
    """Raw input does not satisfy schema or strict quality checks."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "PIPELINE_INPUT_ERROR",
        missing_columns: list[str] | None = None,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.missing_columns = missing_columns or []
        self.details = details or {}
