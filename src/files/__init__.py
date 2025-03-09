"""
Files package for SkypeParser.

This package contains files-related components.
"""

from .tar_extractor import parse_args, main
from .file_utils import safe_filename, get_file_extension, is_json_file, is_tar_file
from .file_output import write_to_file, output_structured_data, export_conversations_to_text, export_conversations
