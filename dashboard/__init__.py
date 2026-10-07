"""Terminal dashboard for the BTC sniper (run.py --dash).

Generic pieces from spe8 (renderer, theme, widgets, safety) plus a layout of
its own: sniper_layout.build turns a plain snapshot dict, built by run.py, into
a frame. Nothing here imports or patches the bot.
"""
from .console import ConsoleCapture
from .renderer import PlainRenderer, Renderer, make_renderer
from .sniper_layout import build, plain_line
from .theme import glyphs

__all__ = ["ConsoleCapture", "PlainRenderer", "Renderer", "build", "glyphs", "make_renderer", "plain_line"]
