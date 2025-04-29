// src/frontend/src/services.js

export async function fetchStreamData() {
  const response = await fetch('/api/stream-data');
  const data = await response.json();
  return data;
}

export async function fetchMessages() {
  const res = await fetch('/api/messages');
  const data = await res.json();
  return data.messages;
}
