import React, { useEffect, useState } from 'react';
import axios from 'axios';
import {
     Chart as ChartJS,
     CategoryScale,
     LinearScale,
     BarElement,
     Title,
     Tooltip,
     Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';

ChartJS.register(
     CategoryScale,
     LinearScale,
     BarElement,
     Title,
     Tooltip,
     Legend
);

const Dashboard = () => {
     const [chartData, setChartData] = useState({
          labels: [],
          datasets: [],
     });

     useEffect(() => {
          const fetchData = async () => {
               try {
                    const response = await axios.get('http://localhost:5000/api/trends');
                    const data = response.data;

                    const labels = data.map((item) => item.name);
                    const counts = data.map((item) => item.count);

                    setChartData({
                         labels,
                         datasets: [
                              {
                                   label: 'Trending Topic Frequency',
                                   data: counts,
                                   backgroundColor: 'rgba(53, 162, 235, 0.5)',
                              },
                         ],
                    });
               } catch (error) {
                    console.error('Error fetching data:', error);
               }
          };

          fetchData();
          const interval = setInterval(fetchData, 2000); // Poll every 2 seconds

          return () => clearInterval(interval);
     }, []);

     const options = {
          responsive: true,
          plugins: {
               legend: {
                    position: 'top',
               },
               title: {
                    display: true,
                    text: 'Real-Time Trending Topics',
               },
          },
     };

     return (
          <div style={{ width: '80%', margin: '0 auto', padding: '20px' }}>
               <h1 style={{ textAlign: 'center' }}>Social Media Trends Dashboard</h1>
               <Bar options={options} data={chartData} />
          </div>
     );
};

export default Dashboard;
