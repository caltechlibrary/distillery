from playwright.sync_api import Page, expect

import pytest

from decouple import config

@pytest.fixture(scope="session")
def browser_context_args(browser_context_args):
    return {
        **browser_context_args,
        "http_credentials": {"username": config("DISTILLERY_BASIC_AUTH_USERNAME", default=""), "password": config("DISTILLERY_BASIC_AUTH_PASSWORD", default="")}
    }

def test_distillery_landing(page: Page):
    page.goto(config("BASE_URL"))
    expect(page).to_have_title("Distillery")
