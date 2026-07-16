# Single Sign-On

Dataflow supports OpenID Connect (OIDC) SSO alongside local username/password
accounts. Microsoft Entra ID is supported through standard OIDC discovery.

On a user's first sign-in, Dataflow can provision a local account and map an
IdP group or role claim to a Dataflow role. The role is refreshed on each
sign-in, keeping the identity provider as the source of truth.

Configure SSO in **Settings -> Single Sign-On**:

| Setting | Purpose |
| --- | --- |
| `sso_enabled` | Enables the provider sign-in option. |
| `sso_discovery_url` | Provider OIDC discovery document. |
| `sso_client_id` and `sso_client_secret` | Application registration credentials. |
| `sso_redirect_uri` | Optional callback override. |
| `sso_group_claim` | Claim containing group IDs or roles. |
| `sso_group_role_map` | JSON mapping from IdP groups to Dataflow roles. |
| `sso_default_role` | Role for users without a group mapping; blank denies access. |

For Entra, register a web application with
`https://<dataflow-host>/auth/sso/callback` as a redirect URI and request
`openid`, `email`, and `profile` scopes.
