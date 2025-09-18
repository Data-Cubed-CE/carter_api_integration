"""
Professional logging system for hotel aggregator with clean console output.
Saves detailed logs to /logs folder with structured format for debugging.
"""

import logging
import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def safe_json_truncate(data: Any, max_length: int) -> str:
    """
    Safely truncate JSON data while preserving valid JSON structure.
    
    Args:
        data: Data to convert to JSON and truncate
        max_length: Maximum length for the resulting string
        
    Returns:
        str: Valid JSON string that's truncated at logical boundaries
    """
    try:
        json_str = json.dumps(data, default=str, ensure_ascii=False)
        if len(json_str) <= max_length:
            return json_str
        
        # Truncate at logical boundaries to maintain valid JSON
        truncated = json_str[:max_length]
        
        # Find last complete key-value pair or array element
        for end_char in ['}', ']', '"', ',']:
            last_pos = truncated.rfind(end_char)
            if last_pos > max_length // 2:  # Ensure meaningful content
                truncated = truncated[:last_pos + 1]
                break
        
        # Add ellipsis and close JSON structure if needed
        if truncated.endswith(','):
            truncated = truncated[:-1]  # Remove trailing comma
        
        # Ensure proper JSON closure
        open_braces = truncated.count('{') - truncated.count('}')
        open_brackets = truncated.count('[') - truncated.count(']')
        
        if open_braces > 0:
            truncated += '...' + '}' * open_braces
        elif open_brackets > 0:
            truncated += '...' + ']' * open_brackets
        else:
            truncated += '...'
            
        return truncated
        
    except Exception:
        # Fallback to string truncation if JSON processing fails
        return str(data)[:max_length] + '...'


class SearchSessionCapture:
    """
    Captures complete search session for full console dump functionality.
    Records all log messages during a search to save complete session to file.
    """
    
    def __init__(self):
        self.current_session = None
        self.session_logs = []
        self.is_capturing = False
        # Check if running in Azure Functions
        self.is_azure = (
            os.getenv("WEBSITE_SITE_NAME") is not None or
            os.getenv("FUNCTIONS_WORKER_RUNTIME") is not None or
            os.getenv("WEBSITE_HOSTNAME") is not None or
            os.getenv("FUNCTIONS_EXTENSION_VERSION") is not None
        )
    
    def start_session(self, search_id: str, search_params: Dict[str, Any]):
        """Start capturing a new search session"""
        self.current_session = {
            "search_id": search_id,
            "start_time": datetime.now(),
            "search_params": search_params,
            "logs": []
        }
        self.session_logs = []
        self.is_capturing = True
    
    def add_log_entry(self, level: str, message: str, provider: str = None):
        """Add a log entry to current session"""
        if self.is_capturing and self.current_session:
            timestamp = datetime.now()
            entry = {
                "timestamp": timestamp,
                "level": level,
                "provider": provider,
                "message": message,
                "formatted": f"{timestamp.strftime('%H:%M:%S')} [{level}] {message}"
            }
            self.session_logs.append(entry)
    
    def end_session(self, results_summary: Dict[str, Any] = None):
        """End the current session and save full dump"""
        if not self.is_capturing or not self.current_session:
            return None
            
        self.current_session["end_time"] = datetime.now()
        self.current_session["duration"] = (
            self.current_session["end_time"] - self.current_session["start_time"]
        ).total_seconds()
        self.current_session["logs"] = self.session_logs
        self.current_session["results_summary"] = results_summary or {}
        
        # Save to file
        dump_file = self._save_session_dump()
        
        # Reset
        self.is_capturing = False
        session_id = self.current_session["search_id"]
        self.current_session = None
        self.session_logs = []
        
        return dump_file
    
    def _save_session_dump(self) -> str:
        """Save complete session dump to file (disabled in Azure Functions)"""
        if not self.current_session:
            return None
        
        # Skip file operations in Azure Functions
        if self.is_azure:
            return None
            
        # Create session dumps directory
        project_root = Path(__file__).parent.parent.parent
        dumps_dir = project_root / "logs" / "session_dumps"
        dumps_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        search_id = self.current_session["search_id"]
        timestamp = self.current_session["start_time"].strftime("%Y%m%d_%H%M%S")
        dump_file = dumps_dir / f"search_session_{search_id}_{timestamp}.txt"
        
        # Generate content
        content = self._generate_dump_content()
        
        # Write to file
        with open(dump_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return str(dump_file)
    
    def _generate_dump_content(self) -> str:
        """Generate formatted dump content"""
        session = self.current_session
        
        content = []
        content.append("=" * 80)
        content.append("HOTEL SEARCH SESSION - FULL CONSOLE DUMP")
        content.append("=" * 80)
        content.append("")
        
        # Session info
        content.append(f"Search ID: {session['search_id']}")
        content.append(f"Start Time: {session['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        content.append(f"End Time: {session['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        content.append(f"Duration: {session['duration']:.2f} seconds")
        content.append("")
        
        # Search parameters
        content.append("SEARCH PARAMETERS:")
        content.append("-" * 40)
        for key, value in session['search_params'].items():
            content.append(f"  {key}: {value}")
        content.append("")
        
        # Results summary
        if session.get('results_summary'):
            content.append("RESULTS SUMMARY:")
            content.append("-" * 40)
            for key, value in session['results_summary'].items():
                content.append(f"  {key}: {value}")
            content.append("")
        
        # Full console log
        content.append("FULL CONSOLE OUTPUT:")
        content.append("-" * 40)
        for log_entry in session['logs']:
            content.append(log_entry['formatted'])
        
        content.append("")
        content.append("=" * 80)
        content.append("END OF SESSION DUMP")
        content.append("=" * 80)
        
        return "\n".join(content)


# Global session capture instance
session_capture = SearchSessionCapture()


class HotelAggregatorLogger:
    """
    Professional logging system for hotel aggregator with clean console output.
    Console logs are concise and emoji-free for better readability.
    Detailed logs are saved to files for debugging.
    """
    
    def __init__(self, name: str = "hotel_aggregator"):
        self.name = name
        
        # Check if running in Azure Functions - use multiple environment variables for reliability
        self.is_azure = (
            os.getenv("WEBSITE_SITE_NAME") is not None or
            os.getenv("FUNCTIONS_WORKER_RUNTIME") is not None or
            os.getenv("WEBSITE_HOSTNAME") is not None or
            os.getenv("FUNCTIONS_EXTENSION_VERSION") is not None
        )
        
        if not self.is_azure:
            # Create logs directory in project root (local development only)
            project_root = Path(__file__).parent.parent.parent
            self.logs_dir = project_root / "logs"
            self.logs_dir.mkdir(exist_ok=True)
            
            # Create daily log files (local development only)
            today = datetime.now().strftime("%Y-%m-%d")
            self.general_log = self.logs_dir / f"general_{today}.log"
            self.data_loss_log = self.logs_dir / f"data_loss_{today}.log"
            self.provider_log = self.logs_dir / f"providers_{today}.log"
            self.debug_log = self.logs_dir / f"debug_{today}.log"
        else:
            # In Azure Functions, disable file logging
            self.logs_dir = None
            self.general_log = None
            self.data_loss_log = None
            self.provider_log = None
            self.debug_log = None
        
        self._setup_loggers()
    
    def _setup_loggers(self):
        """Setup different loggers for different purposes"""
        
        # Clear any existing handlers to avoid duplicates
        for logger_name in [f"{self.name}_general", f"{self.name}_data_loss", 
                           f"{self.name}_providers", f"{self.name}_debug"]:
            logger = logging.getLogger(logger_name)
            logger.handlers.clear()
        
        # General application logger
        self.general_logger = logging.getLogger(f"{self.name}_general")
        self.general_logger.setLevel(logging.INFO)
        
        # Data loss tracking logger
        self.data_loss_logger = logging.getLogger(f"{self.name}_data_loss")
        self.data_loss_logger.setLevel(logging.DEBUG)
        
        # Provider-specific logger
        self.provider_logger = logging.getLogger(f"{self.name}_providers")
        self.provider_logger.setLevel(logging.DEBUG)
        
        # Debug logger for detailed debugging
        self.debug_logger = logging.getLogger(f"{self.name}_debug")
        self.debug_logger.setLevel(logging.DEBUG)
        
        # Create formatters
        # File formatter - detailed
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        # Console formatter - clean and professional
        console_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # Setup file handlers (detailed logging) - only in local development
        if not self.is_azure:
            self._add_file_handler(self.general_logger, self.general_log, file_formatter)
            self._add_file_handler(self.data_loss_logger, self.data_loss_log, file_formatter)
            self._add_file_handler(self.provider_logger, self.provider_log, file_formatter)
            self._add_file_handler(self.debug_logger, self.debug_log, file_formatter)
        
        # Console handler - clean output only for important messages
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)
        self.general_logger.addHandler(console_handler)
        
        # Prevent log propagation to avoid duplicate messages
        self.general_logger.propagate = False
        self.data_loss_logger.propagate = False
        self.provider_logger.propagate = False
        self.debug_logger.propagate = False
    
    def _add_file_handler(self, logger, log_file, formatter):
        """Add file handler to logger (skip if log_file is None for Azure Functions)"""
        if log_file is not None:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    
    def log_data_processing_start(self, provider: str, total_items: int, context: Dict[str, Any] = None):
        """Log start of data processing"""
        # Clean console message
        console_msg = f"{provider}: Starting search with {total_items} items"
        self.general_logger.info(console_msg)
        self._capture_log_if_session_active("INFO", console_msg, provider)
        
        # Detailed file message
        msg = f"{provider}: Starting data processing - {total_items} total items"
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        self.provider_logger.info(msg)
    
    def log_data_processing_end(self, provider: str, processed: int, skipped: int, success: int, 
                               total: int, processing_time_ms: float = None):
        """Log end of data processing with detailed stats"""
        success_rate = (success / total * 100) if total > 0 else 0
        
        # Clean console message
        self.general_logger.info(f"{provider}: {success}/{total} offers ({success_rate:.1f}%)")
        
        # Detailed file message
        msg = (f"{provider}: Processing complete - "
               f"{success}/{total} offers created ({success_rate:.1f}% success rate) | "
               f"Processed: {processed}, Skipped: {skipped}")
        
        if processing_time_ms:
            msg += f" | Time: {processing_time_ms:.0f}ms"
        
        self.provider_logger.info(msg)
        
        # Log to data loss if significant loss detected
        if success_rate < 80:
            self.data_loss_logger.warning(
                f"{provider}: High data loss detected - only {success_rate:.1f}% success rate"
            )
    
    def log_skipped_item(self, provider: str, item_index: int, reason: str, 
                        item_data: Dict[str, Any] = None, item_id: str = None):
        """Log individual skipped items with reason"""
        identifier = item_id or f"item_{item_index}"
        msg = f"{provider}: Skipped {identifier} - {reason}"
        
        self.data_loss_logger.info(msg)
        
        # Log detailed item data for debugging (only first few keys to avoid log spam)
        if item_data:
            # Log only essential keys to keep logs readable
            essential_keys = ['match_hash', 'room_name', 'HotelSearchCode', 'TotalPrice', 'legal_info']
            essential_data = {k: v for k, v in item_data.items() if k in essential_keys}
            
            self.debug_logger.debug(
                f"{provider}: Skipped item data for {identifier}: "
                f"{json.dumps(essential_data, indent=2, default=str, ensure_ascii=False)}"
            )
    
    def log_validation_error(self, provider: str, item_index: int, errors: List[str], 
                           item_data: Dict[str, Any] = None, item_id: str = None):
        """Log validation errors with details"""
        identifier = item_id or f"item_{item_index}"
        msg = f"{provider}: Validation failed for {identifier} - {len(errors)} errors"
        
        self.data_loss_logger.error(msg)
        
        # Log detailed validation errors
        for i, error in enumerate(errors):
            self.data_loss_logger.error(f"   {i+1}. {error}")
        
        # Log problematic data (limited keys)
        if item_data:
            # Only log first few keys to understand the structure
            sample_data = dict(list(item_data.items())[:5])
            self.debug_logger.debug(
                f"{provider}: Failed validation data sample for {identifier}: "
                f"{json.dumps(sample_data, indent=2, default=str, ensure_ascii=False)}"
            )
    
    def log_provider_summary(self, provider: str, raw_count: int, normalized_count: int, 
                           final_count: int, missing_data_stats: Dict[str, int] = None):
        """Log provider processing summary"""
        # Clean console message
        self.general_logger.info(f"{provider} Summary: {raw_count} -> {final_count}")
        
        # Detailed file message
        msg = (f"{provider} SUMMARY: "
               f"Raw: {raw_count} -> Normalized: {normalized_count} -> Final: {final_count}")
        
        if missing_data_stats:
            stats_str = json.dumps(missing_data_stats, ensure_ascii=False)
            msg += f" | Missing data: {stats_str}"
        
        self.provider_logger.info(msg)
        
        # Calculate loss percentages
        if raw_count > 0:
            normalization_loss = ((raw_count - normalized_count) / raw_count * 100)
            final_loss = ((raw_count - final_count) / raw_count * 100)
            
            if normalization_loss > 10:
                self.data_loss_logger.warning(
                    f"{provider}: High normalization loss - {normalization_loss:.1f}%"
                )
            
            if final_loss > 15:
                self.data_loss_logger.error(
                    f"{provider}: Critical final data loss - {final_loss:.1f}%"
                )
    
    def log_raw_response_size(self, provider: str, response_data: Dict[str, Any]):
        """Log raw response analysis"""
        try:
            if provider == "rate_hawk":
                hotels = response_data.get("data", {}).get("hotels", [])
                total_rates = sum(len(hotel.get("rates", [])) for hotel in hotels)
                msg = f"{provider}: Raw response - {len(hotels)} hotels, {total_rates} total rates"
                
            elif provider == "goglobal":
                hotels = response_data.get("Hotels", [])
                total_offers = sum(len(hotel.get("Offers", [])) for hotel in hotels)
                msg = f"{provider}: Raw response - {len(hotels)} hotels, {total_offers} total offers"
            else:
                msg = f"{provider}: Raw response received"
            
            self.provider_logger.info(msg)
            self.general_logger.info(msg)
            
        except Exception as e:
            self.general_logger.error(f"Error analyzing raw response for {provider}: {e}")

    def log_offer_creation_attempt(self, provider: str, offer_data: Dict[str, Any], success: bool, error: str = None):
        """Log individual offer creation attempts"""
        identifier = (offer_data.get('match_hash') or 
                     offer_data.get('HotelSearchCode') or 
                     offer_data.get('supplier_room_code') or 
                     'unknown')
        
        if success:
            self.debug_logger.debug(f"{provider}: Successfully created offer {identifier}")
        else:
            self.data_loss_logger.error(f"{provider}: Failed to create offer {identifier} - {error}")

    def get_log_files_info(self) -> Dict[str, Any]:
        """Get information about log files"""
        info = {
            "logs_directory": str(self.logs_dir),
            "log_files": {
                "general": str(self.general_log),
                "data_loss": str(self.data_loss_log),
                "providers": str(self.provider_log),
                "debug": str(self.debug_log)
            },
            "file_sizes": {}
        }
        
        for name, path in info["log_files"].items():
            try:
                size = os.path.getsize(path)
                info["file_sizes"][name] = f"{size} bytes"
            except FileNotFoundError:
                info["file_sizes"][name] = "File not created yet"
        
        return info
    
    def log_data_processing_complete(self, provider: str, processed_count: int, 
                                   success_count: int, error_count: int, 
                                   processing_time: float, context: Dict[str, Any] = None):
        """Log completion of data processing with metrics"""
        # Clean console message
        self.general_logger.info(f"{provider}: Completed - {success_count}/{processed_count} success")
        
        # Detailed file message
        msg = f"{provider}: Data processing complete - {processed_count} processed"
        msg += f" | Success: {success_count} | Errors: {error_count}"
        msg += f" | Time: {processing_time:.2f}s"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.provider_logger.info(msg)
    
    def log_data_loss(self, provider: str, issue_type: str, lost_data: Any, 
                     reason: str, severity: str = "WARNING", context: Dict[str, Any] = None):
        """Log data loss incidents with full details"""
        # Clean console message for severe cases only
        if severity in ["ERROR", "CRITICAL"]:
            self.general_logger.warning(f"Data Loss - {provider}: {issue_type}")
        
        # Detailed file message
        msg = f"DATA LOSS - {provider} | Type: {issue_type} | Severity: {severity}"
        msg += f" | Reason: {reason}"
        
        if isinstance(lost_data, (dict, list)):
            lost_data_str = safe_json_truncate(lost_data, 500)
            msg += f" | Lost Data: {lost_data_str}"
        else:
            msg += f" | Lost Data: {str(lost_data)[:500]}"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.data_loss_logger.warning(msg)
    
    def log_provider_response(self, provider: str, status_code: int, response_time: float, 
                            offer_count: int, response_size: int = None, 
                            context: Dict[str, Any] = None):
        """Log provider response with metrics"""
        # Clean console message
        if status_code >= 400:
            console_msg = f"{provider}: HTTP {status_code} - {offer_count} offers"
            self.general_logger.warning(console_msg)
            self._capture_log_if_session_active("WARNING", console_msg, provider)
        else:
            console_msg = f"{provider}: {offer_count} offers in {response_time:.1f}s"
            self.general_logger.info(console_msg)
            self._capture_log_if_session_active("INFO", console_msg, provider)
        
        # Detailed file message
        msg = f"{provider} Response: {status_code} | Time: {response_time:.2f}s"
        msg += f" | Offers: {offer_count}"
        
        if response_size:
            msg += f" | Size: {response_size} bytes"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.provider_logger.info(msg)
    
    def log_mapping_operation(self, operation: str, provider: str, input_value: str, 
                            output_value: str = None, success: bool = True, 
                            context: Dict[str, Any] = None):
        """Log mapping operations (hotel/room mapping)"""
        if success:
            msg = f"{operation} - {provider}: '{input_value}'"
            if output_value:
                msg += f" -> '{output_value}'"
        else:
            msg = f"{operation} FAILED - {provider}: '{input_value}'"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.provider_logger.debug(msg)
    
    def log_circuit_breaker_event(self, provider: str, event_type: str, 
                                 failure_count: int = None, timeout: float = None,
                                 context: Dict[str, Any] = None):
        """Log circuit breaker events"""
        # Console message for important events
        self.general_logger.warning(f"Circuit Breaker - {provider}: {event_type}")
        
        # Detailed file message
        msg = f"Circuit Breaker - {provider}: {event_type}"
        
        if failure_count is not None:
            msg += f" | Failures: {failure_count}"
        if timeout:
            msg += f" | Timeout: {timeout}s"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.provider_logger.warning(msg)
    
    def log_aggregation_summary(self, total_providers: int, successful_providers: int,
                              total_offers: int, unique_hotels: int, processing_time: float,
                              context: Dict[str, Any] = None):
        """Log final aggregation summary"""
        # Clean console message
        console_msg = f"Search completed: {successful_providers}/{total_providers} providers"
        console_msg += f" | {unique_hotels} hotels | {total_offers} offers | {processing_time:.1f}s"
        self.general_logger.info(console_msg)
        self._capture_log_if_session_active("INFO", console_msg)
        
        # Detailed file message
        msg = f"Aggregation Summary: {successful_providers}/{total_providers} providers"
        msg += f" | Hotels: {unique_hotels} | Offers: {total_offers}"
        msg += f" | Total time: {processing_time:.2f}s"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.provider_logger.info(msg)
        
        # Auto-save session dump when search completes
        if session_capture.is_capturing:
            results_summary = {
                "total_providers": total_providers,
                "successful_providers": successful_providers,
                "total_offers": total_offers,
                "unique_hotels": unique_hotels,
                "processing_time_seconds": processing_time
            }
            self.end_search_session(results_summary)
    
    def log_error(self, provider: str, error_type: str, error_message: str,
                 context: Dict[str, Any] = None, exc_info: bool = False):
        """Log errors with context"""
        # Clean console message
        self.general_logger.error(f"{provider}: {error_type}")
        
        # Detailed file message
        msg = f"ERROR - {provider}: {error_type} | {error_message}"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.provider_logger.error(msg, exc_info=exc_info)
    
    def log_debug(self, provider: str, operation: str, details: Any, 
                 context: Dict[str, Any] = None):
        """Log debug information"""
        msg = f"DEBUG - {provider}: {operation}"
        
        if isinstance(details, (dict, list)):
            details_str = safe_json_truncate(details, 1000)
            msg += f" | Details: {details_str}"
        else:
            msg += f" | Details: {str(details)[:1000]}"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.debug_logger.debug(msg)
    
    def log_performance_metric(self, metric_name: str, value: float, unit: str = "ms",
                             provider: str = None, context: Dict[str, Any] = None):
        """Log performance metrics"""
        msg = f"PERFORMANCE: {metric_name} = {value}{unit}"
        if provider:
            msg = f"PERFORMANCE - {provider}: {metric_name} = {value}{unit}"
        
        if context:
            context_str = json.dumps(context, default=str, ensure_ascii=False)
            msg += f" | Context: {context_str}"
        
        self.debug_logger.info(msg)
    
    def log_info(self, message: str, provider: str = None):
        """Log general information"""
        if provider:
            msg = f"{provider}: {message}"
        else:
            msg = message
        
        self.general_logger.info(msg)
        self._capture_log_if_session_active("INFO", msg, provider)
    
    def log_warning(self, message: str, provider: str = None):
        """Log warnings"""
        if provider:
            msg = f"WARNING - {provider}: {message}"
        else:
            msg = f"WARNING: {message}"
        
        self.general_logger.warning(msg)
        self._capture_log_if_session_active("WARNING", msg, provider)
    
    # Standard logging interface methods
    def info(self, message: str, provider: str = None):
        """Standard info logging method"""
        self.log_info(message, provider)
    
    def warning(self, message: str, provider: str = None):
        """Standard warning logging method"""
        self.log_warning(message, provider)
    
    def error(self, message: str, exc_info: bool = False, provider: str = None):
        """Standard error logging method"""
        if provider:
            msg = f"ERROR - {provider}: {message}"
        else:
            msg = f"ERROR: {message}"
        
        self.general_logger.error(msg, exc_info=exc_info)
        self._capture_log_if_session_active("ERROR", msg, provider)
    
    def debug(self, message: str, provider: str = None):
        """Standard debug logging method"""
        if provider:
            msg = f"DEBUG - {provider}: {message}"
        else:
            msg = f"DEBUG: {message}"
        
        self.debug_logger.debug(msg)
        self._capture_log_if_session_active("DEBUG", msg, provider)
    
    def critical(self, message: str, exc_info: bool = False, provider: str = None):
        """Standard critical logging method"""
        if provider:
            msg = f"CRITICAL - {provider}: {message}"
        else:
            msg = f"CRITICAL: {message}"
        
        self.general_logger.critical(msg, exc_info=exc_info)
        self._capture_log_if_session_active("CRITICAL", msg, provider)
    
    def start_search_session(self, search_id: str, search_params: Dict[str, Any]):
        """Start capturing complete search session for console dump"""
        session_capture.start_session(search_id, search_params)
        self.log_info(f"Started search session: {search_id}")
    
    def end_search_session(self, results_summary: Dict[str, Any] = None) -> str:
        """End search session and save complete console dump"""
        dump_file = session_capture.end_session(results_summary)
        if dump_file:
            self.log_info(f"Search session saved to: {dump_file}")
            return dump_file
        return None
    
    def _capture_log_if_session_active(self, level: str, message: str, provider: str = None):
        """Capture log entry if session is active"""
        if session_capture.is_capturing:
            session_capture.add_log_entry(level, message, provider)


# Global logger instance
hotel_logger = HotelAggregatorLogger()


def get_logger(name: str = "hotel_aggregator") -> HotelAggregatorLogger:
    """Get the global hotel logger instance"""
    return hotel_logger


# Convenience functions for backward compatibility
def log_info(message: str):
    """Log general info message"""
    hotel_logger.general_logger.info(message)

def log_error(message: str):
    """Log general error message"""
    hotel_logger.general_logger.error(message)

def log_debug(message: str):
    """Log debug message"""
    hotel_logger.debug_logger.debug(message)
