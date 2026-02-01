"""Alerts module for notification system."""

from .telegram import TelegramAlerter, alerter

__all__ = ["TelegramAlerter", "alerter"]
