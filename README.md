# MongoDB Message Queue - Python Implementation

![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-4.0%2B-green)
![License](https://img.shields.io/badge/license-MIT-orange)

A robust, production-ready message queue/service bus implementation using MongoDB as the backing store.

## Features

- ✅ **Channel-based Isolation** - Strict message separation by channels
- 🔢 **Guaranteed Ordering** - Atomic sequence numbers per channel
- 🔄 **Automatic Retries** - Configurable retry logic for failed operations
- 💓 **Heartbeat Monitoring** - Detects stalled consumers
- ⏱️ **Visibility Timeout** - Configurable processing time limits
- 📊 **Monitoring** - Channel statistics and detailed logging
- 🔒 **Thread-safe** - Safe for concurrent access

## Installation

```bash
pip install pymongo python-dotenv