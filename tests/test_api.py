import pytest


def test_meta(client):
    response = client.get('/meta')
    assert response.status_code == 404
