import pendulum

from hyprxa.__version__ import __title__ as TITLE



DEFAULT_APPNAME = TITLE
DEFAULT_DATABASE = TITLE
DEFAULT_TIMEZONE = pendulum.now().timezone_name