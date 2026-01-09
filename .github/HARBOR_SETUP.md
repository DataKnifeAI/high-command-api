# Harbor Registry Setup

This project uses Harbor registry at `harbor.dataknife.net` for Docker image storage.

## GitHub Secrets Configuration

To enable automated builds and pushes to Harbor, you need to configure the following secrets in your GitHub repository:

### Required Secrets

1. **HARBOR_USERNAME**: Harbor robot account username

2. **HARBOR_PASSWORD**: Harbor robot account password/token

### Setting up GitHub Secrets

You can set secrets using the GitHub CLI:

```bash
gh secret set HARBOR_USERNAME --repo <owner>/<repo> --body "your-harbor-username"
gh secret set HARBOR_PASSWORD --repo <owner>/<repo> --body "your-harbor-password"
```

Or via the web UI:
1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add the secrets (values should be obtained from your Harbor administrator)

## Local Development

For local development, you can set environment variables:

```bash
export HARBOR_USERNAME='your-harbor-username'
export HARBOR_PASSWORD='your-harbor-password'
```

Then use the Makefile targets:

```bash
# Login to Harbor
make docker-login

# Build and push images
make docker-push

# Pull images
make docker-pull
```

## Docker Image Location

The image is stored in the Harbor registry:
- `harbor.dataknife.net/library/high-command-api:latest`

## Using Docker Compose

Docker Compose is configured to pull images from Harbor automatically. Just run:

```bash
docker-compose pull
docker-compose up -d
```

Make sure you're logged into Harbor first:

```bash
docker login harbor.dataknife.net \
  -u 'your-harbor-username' \
  -p 'your-harbor-password'
```
