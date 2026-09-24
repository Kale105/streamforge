-- Extend the original API-key scope check without changing existing grants.
ALTER TABLE api_key_scopes DROP CONSTRAINT IF EXISTS api_key_scopes_action_check;
ALTER TABLE api_key_scopes ADD CONSTRAINT api_key_scopes_action_check
    CHECK (action IN ('read', 'write', 'provider-admin'));
