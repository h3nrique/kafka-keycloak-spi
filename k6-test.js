import http from 'k6/http';
import { check, sleep } from 'k6';

const KEYCLOAK_URL = 'http://localhost:8280';
const REALM = 'example';
const CLIENT_ID = 'example';
const USERNAME = 'otto';
const PASSWORD = 'otto';

export let options = {
  vus: 300,
  duration: '20s',
};

export default function () {
  const url = `${KEYCLOAK_URL}/realms/${REALM}/protocol/openid-connect/token`;
  const payload = {
    grant_type: 'password',
    client_id: CLIENT_ID,
    username: USERNAME,
    password: PASSWORD,
    scope: 'openid',
  };
  const params = {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
  };

  const res = http.post(url, payload, params);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'has access token': (r) => r.json() && r.json().access_token,
  });

  sleep(1);
}
