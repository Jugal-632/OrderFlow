"""Generate disposable local credentials without printing or replacing existing secrets."""
import os
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SECRET_KEYS = ('POSTGRES_PASSWORD', 'JWT_SECRET', 'INTERNAL_SERVICE_TOKEN',
               'BOOTSTRAP_CUSTOMER_PASSWORD', 'BOOTSTRAP_ADMIN_PASSWORD')

def read_env(path):
    values = {}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values

def credentials():
    path = ROOT / '.env'
    values = read_env(path) if path.exists() else {}
    values.update({key: os.environ[key] for key in (*SECRET_KEYS, 'BOOTSTRAP_ADMIN_EMAIL',
                  'BOOTSTRAP_CUSTOMER_EMAIL') if key in os.environ})
    for key in SECRET_KEYS:
        if not values.get(key):
            raise ValueError('Missing ' + key + '; run python scripts/local_env.py')
    return values

def generate(root=ROOT):
    path = root / '.env'
    template = read_env(root / '.env.example')
    current = read_env(path) if path.exists() else {}
    additions = {key: (secrets.token_hex(32) if key in SECRET_KEYS else value)
                 for key, value in template.items() if not current.get(key)}
    if additions:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(fd, 'a', encoding='utf-8') as stream:
            stream.write('\n# Generated local settings; do not commit.\n')
            for key, value in additions.items():
                stream.write(key + '=' + value + '\n')
    return len(additions)

if __name__ == '__main__':
    print('Local environment ready; added', generate(), 'settings. Existing values preserved.')
