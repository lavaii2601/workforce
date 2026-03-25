from .attendance_routes import register_attendance_routes
from .general_routes import register_general_routes
from .leadership_routes import register_leadership_routes
from .operations_routes import register_operations_routes

__all__ = [
	"register_attendance_routes",
	"register_general_routes",
	"register_leadership_routes",
	"register_operations_routes",
]
