CREATE TABLE user_roles (
    user_id UUID NOT NULL REFERENCES users(id),
    role VARCHAR(16) NOT NULL CHECK (role IN ('CUSTOMER','ADMIN')),
    PRIMARY KEY(user_id,role)
);
INSERT INTO user_roles(user_id,role) SELECT id,'CUSTOMER' FROM users ON CONFLICT DO NOTHING;
