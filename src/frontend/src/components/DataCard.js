// src/frontend/src/components/DataCard.js
import React from 'react';

const DataCard = ({ item }) => (
  <div className="card">
    <p><strong>ID:</strong> {item.id}</p>
    <p><strong>Message:</strong> {item.message}</p>
    <p><strong>Timestamp:</strong> {item.timestamp}</p>
  </div>
);

export default DataCard;
