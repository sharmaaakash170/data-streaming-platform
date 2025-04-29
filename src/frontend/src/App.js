import React, { useEffect, useState } from 'react';
import { fetchStreamData } from './services';
import DataCard from './components/DataCard';


const App = () => {
  const [data, setData] = useState([]);

  useEffect(() => {
    const interval = setInterval(() => {
      fetchStreamData().then(setData).catch(console.error);
    }, 5000); // fetch every 5 seconds
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="container">
      <h1>Kafka Stream Viewer</h1>
      {data.length === 0 ? <p>No data available yet.</p> : (
        data.map((item, idx) => <DataCard key={idx} item={item} />)
      )}
    </div>
  );
};

export default App;
