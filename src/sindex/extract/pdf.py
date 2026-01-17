from __future__ import annotations


def extract_text_from_pdf(path: str) -> str:
    """
    Extract text from a PDF.
    """
    from pdfminer.high_level import extract_text  # pdfminer.six

    try:
        return extract_text(path) or ""
    except Exception:
        return ""
