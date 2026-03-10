TLS certificates for nginx (prod profile)

The prod nginx config serves HTTPS only and redirects HTTP→HTTPS. Certificates are mounted from `platform/certs` into the container at `/etc/nginx/certs` and are referenced by all prod virtual hosts.

Required files:

- `platform/certs/fullchain.pem`  (public certificate chain)
- `platform/certs/privkey.pem`    (private key)

Important
- Let's Encrypt does not issue certificates for `.localhost` domains. Replace the example domains below with your real production hostnames that point to your server: `hasura.prod.localhost`, `dagster.prod.localhost`, `superset.prod.localhost` → `hasura.example.com`, `dagster.example.com`, `superset.example.com`, etc.
- The current nginx config expects a single certificate that covers all prod hosts. Obtain either:
  - a single SAN certificate including all hostnames, or
  - a wildcard certificate (e.g., `*.example.com`).

Prerequisites
1) DNS A/AAAA records for each hostname pointing to the server's public IP
2) Ports 80 and 443 open to the internet (for HTTP-01 challenges)
3) Shell access to the production host

Option A: Single SAN certificate via HTTP-01 (standalone)
This temporarily stops nginx to allow Certbot to bind to :80/:443.

```bash
# Replace these with your real domains; the first becomes the certificate name
PRIMARY=hasura.example.com
DOMAINS="hasura.example.com,dagster.example.com,superset.example.com"
EMAIL=admin@example.com

cd platform
docker compose stop nginx

# Run Certbot on the host (install via your package manager) OR in a container:
sudo certbot certonly --standalone \
  -d ${DOMAINS//,/ -d } \
  --agree-tos --email "$EMAIL" --non-interactive

# Copy the issued cert and key into the project certs folder
mkdir -p certs
sudo cp /etc/letsencrypt/live/$PRIMARY/fullchain.pem certs/fullchain.pem
sudo cp /etc/letsencrypt/live/$PRIMARY/privkey.pem   certs/privkey.pem

# Restart only nginx
docker compose up -d --no-deps --build nginx
```

Option B: Wildcard certificate via DNS-01
This works without stopping nginx but requires a DNS provider plugin or manual DNS TXT records.

```bash
# Example using manual DNS (you'll be prompted to add TXT records)
BASE_DOMAIN=example.com
EMAIL=admin@example.com

sudo certbot certonly --manual --preferred-challenges dns \
  -d "*.${BASE_DOMAIN}" -d "${BASE_DOMAIN}" \
  --agree-tos --email "$EMAIL" --no-eff-email

mkdir -p platform/certs
sudo cp /etc/letsencrypt/live/${BASE_DOMAIN}/fullchain.pem platform/certs/fullchain.pem
sudo cp /etc/letsencrypt/live/${BASE_DOMAIN}/privkey.pem   platform/certs/privkey.pem

cd platform && docker compose up -d --no-deps --build nginx
```

Renewal
- Let’s Encrypt certificates expire every 90 days. On the server, set a cron job to run `certbot renew` daily.
- After a successful renewal, re-copy the renewed files into `platform/certs` and reload nginx:

```bash
sudo certbot renew --quiet
sudo cp /etc/letsencrypt/live/<CERT_NAME>/fullchain.pem /path/to/repo/platform/certs/fullchain.pem
sudo cp /etc/letsencrypt/live/<CERT_NAME>/privkey.pem   /path/to/repo/platform/certs/privkey.pem
cd /path/to/repo/platform && docker compose exec nginx nginx -s reload || docker compose restart nginx
```

Local development (self-signed)
For local testing only (browsers will warn):

```bash
cd platform
mkdir -p certs
openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
  -keyout certs/privkey.pem -out certs/fullchain.pem \
  -subj "/CN=*.prod.localhost"

docker compose up -d --no-deps --build nginx
```

Validation
```bash
# Expect HTTP 301 to https
curl -I http://dagster.example.com | head -n 5

# Expect a valid certificate chain and no Server header leak
curl -I https://dagster.example.com | head -n 15
```