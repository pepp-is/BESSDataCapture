const mqtt = require('mqtt');
const { createClient } = require('@supabase/supabase-js');

// MQTT broker configuration
const brokerUrl = 'mqtt://broker.emqx.io:1883';
const mqttOptions = {};

const supabaseUrl = 'https://vxmpvmacrmfmonzfcgud.supabase.co';
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZ4bXB2bWFjcm1mbW9uemZjZ3VkIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDEyMzc4NDIsImV4cCI6MjA1NjgxMzg0Mn0.KDB9pL0zbv-S_7RijJYOtmEWzRHV_jzngb8Y3Qok9Ww'; // Replace with secure key in production
const supabase = createClient(supabaseUrl, supabaseKey);

const client = mqtt.connect(brokerUrl, mqttOptions);
let latestData = {};

client.on('connect', () => {
  console.log('Connected to MQTT broker');

  // Subscribe to all device topics: BE/deviceXYZ/info
  client.subscribe('BE/+/info', (err) => {
    if (err) {
      console.error('Subscription error:', err);
    } else {
      console.log('Subscribed to BE/+/info');
    }
  });
});

client.on('message', async (topic, payload) => {
  try {
    const data = JSON.parse(payload.toString());

    // Extract device ID from topic: "BE/device123/info"
    const parts = topic.split('/');
    const deviceId = parts[1]; // "device123"

    // Ensure device is recorded in `devices` table (optional but useful for tracking)
    await supabase
      .from('devices')
      .upsert([{ device_id: deviceId }], { onConflict: ['device_id'] });

    // Insert the actual data with the device ID
    const { error } = await supabase.from('bess_data3').insert([
      {
        device_id: deviceId,
        bms_status: data.bms_status,
        pause_status: data.pause_status,
        soc: data.SOC,
        soc_real: data.SOC_real,
        state_of_health: data.state_of_health,
        temperature_min: data.temperature_min,
        temperature_max: data.temperature_max,
        stat_batt_power: data.stat_batt_power,
        battery_current: data.battery_current,
        battery_voltage: data.battery_voltage,
        total_capacity: data.total_capacity,
        remaining_capacity_real: data.remaining_capacity_real,
        remaining_capacity: data.remaining_capacity,
        max_discharge_power: data.max_discharge_power,
        max_charge_power: data.max_charge_power,
      },
    ]);

    if (error) {
      console.error('Insert error:', error);
    } else {
      console.log(`Data from ${deviceId} inserted into Supabase`);
    }

    latestData[deviceId] = data;
  } catch (err) {
    console.error('Failed to process MQTT message:', err);
  }
});

client.on('error', (err) => {
  console.error('MQTT error:', err);
});

client.on('close', () => {
  console.log('MQTT connection closed');
});

function getLatestData(deviceId) {
  return latestData[deviceId] || null;
}

module.exports = { getLatestData };
