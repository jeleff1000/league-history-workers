from pathlib import Path


def test_mfl_roster_bridge_pilot_installs_the_private_mfl_client_dependencies() -> None:
    workflow = (
        Path(__file__).resolve().parents[2]
        / ".github" / "workflows" / "research_mfl_roster_bridge_pilot.yml"
    ).read_text(encoding="utf-8")

    assert "requests==2.32.4" in workflow
    assert "polars==1.35.2" in workflow
