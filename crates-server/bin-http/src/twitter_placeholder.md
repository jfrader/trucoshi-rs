This repo currently implements placeholder Twitter OAuth2 endpoints:

- GET /v1/auth/twitter
- GET /v1/auth/twitter/callback

They DO NOT perform real OAuth yet.

To enable real Twitter OAuth2, set:
- PUBLIC_BASE_URL
- TWITTER_CLIENT_ID
- TWITTER_CLIENT_SECRET

And implement the OAuth2 exchange + userinfo fetch.
