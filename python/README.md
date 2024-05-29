# About

## Dependencies

### Python venv

```shell
mkdir -p ~/.env && python3 -m venv ~/.env
```

### Install

```shell
~/.env/bin/python -m pip install -r ./requirements.txt
```
## Test

```shell
~/.env/bin/python -m unittest discover tests "*_test.py"
```

## Usage

```shell
~/.env/bin/python cli.py
```
