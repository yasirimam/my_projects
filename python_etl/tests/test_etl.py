from portfolio_etl.etl import load_config

def test_config():
    cfg = load_config()
    assert "warehouse_path" in cfg
