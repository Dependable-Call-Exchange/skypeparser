# Building a Web App with SkypeParser

This guide explains how to build a web application that uses SkypeParser data stored in a database.

## Overview

SkypeParser provides a robust backend for processing and storing Skype data. By building a web application on top of this data, you can create a user-friendly interface for browsing, searching, and analyzing your Skype conversations.

## Prerequisites

Before building a web app with SkypeParser, you need:

1. Skype data processed and stored in a database (PostgreSQL or Supabase)
2. Basic knowledge of web development (HTML, CSS, JavaScript)
3. Familiarity with a web framework (React, Vue, Angular, etc.)

## Architecture Options

There are several ways to build a web app with SkypeParser:

### Option 1: REST API + Frontend

This approach involves building a REST API that accesses the SkypeParser database and a separate frontend application that consumes the API.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  SkypeParser │     │  REST API   │     │  Frontend   │
│  Database    │────▶│  (Flask/    │────▶│  (React/    │
│              │     │   FastAPI)  │     │   Vue)      │
└─────────────┘     └─────────────┘     └─────────────┘
```

### Option 2: Full-Stack Framework

This approach uses a full-stack framework that handles both the backend and frontend.

```
┌─────────────┐     ┌─────────────────────────────┐
│  SkypeParser │     │  Full-Stack Framework       │
│  Database    │────▶│  (Django, Next.js, etc.)    │
│              │     │                             │
└─────────────┘     └─────────────────────────────┘
```

### Option 3: Supabase + Frontend

If you're using Supabase, you can leverage its built-in features to build a web app without a custom backend.

```
┌─────────────┐     ┌─────────────┐
│  Supabase   │     │  Frontend   │
│  (Database, │────▶│  (React/    │
│   Auth, API)│     │   Vue)      │
└─────────────┘     └─────────────┘
```

## Building a REST API

### Using Flask

Here's an example of a simple Flask API for SkypeParser:

```python
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

app = Flask(__name__)
CORS(app)

def get_db_connection():
    conn = psycopg2.connect(
        host='localhost',
        database='skype_data',
        user='postgres',
        password='password'
    )
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn

@app.route('/api/conversations', methods=['GET'])
def get_conversations():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT * FROM skype_conversations ORDER BY display_name')
    conversations = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(conversations)

@app.route('/api/conversations/<conversation_id>/messages', methods=['GET'])
def get_messages(conversation_id):
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    offset = (page - 1) * per_page

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT * FROM skype_messages
        WHERE conversation_id = %s
        ORDER BY timestamp
        LIMIT %s OFFSET %s
        ''',
        (conversation_id, per_page, offset)
    )
    messages = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(messages)

@app.route('/api/search', methods=['GET'])
def search_messages():
    query = request.args.get('q', '')

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        '''
        SELECT m.*, c.display_name as conversation_name
        FROM skype_messages m
        JOIN skype_conversations c ON m.conversation_id = c.conversation_id
        WHERE m.content ILIKE %s
        ORDER BY m.timestamp DESC
        LIMIT 100
        ''',
        (f'%{query}%',)
    )
    results = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
```

### Using FastAPI

Here's an example of a simple FastAPI API for SkypeParser:

```python
from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import List, Optional
import os
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
SQLALCHEMY_DATABASE_URL = "postgresql://postgres:password@localhost/skype_data"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Models
class Conversation(BaseModel):
    conversation_id: str
    display_name: str
    first_message_time: Optional[str] = None
    last_message_time: Optional[str] = None
    message_count: Optional[int] = None

class Message(BaseModel):
    message_id: str
    conversation_id: str
    timestamp: str
    sender_name: str
    content: str
    message_type: str
    is_edited: bool

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/api/conversations", response_model=List[Conversation])
def get_conversations(db = Depends(get_db)):
    result = db.execute(text("SELECT * FROM skype_conversations ORDER BY display_name"))
    return result.fetchall()

@app.get("/api/conversations/{conversation_id}/messages", response_model=List[Message])
def get_messages(
    conversation_id: str,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=100),
    db = Depends(get_db)
):
    offset = (page - 1) * per_page
    result = db.execute(
        text(
            """
            SELECT * FROM skype_messages
            WHERE conversation_id = :conversation_id
            ORDER BY timestamp
            LIMIT :per_page OFFSET :offset
            """
        ),
        {"conversation_id": conversation_id, "per_page": per_page, "offset": offset}
    )
    return result.fetchall()

@app.get("/api/search")
def search_messages(q: str, db = Depends(get_db)):
    result = db.execute(
        text(
            """
            SELECT m.*, c.display_name as conversation_name
            FROM skype_messages m
            JOIN skype_conversations c ON m.conversation_id = c.conversation_id
            WHERE m.content ILIKE :query
            ORDER BY m.timestamp DESC
            LIMIT 100
            """
        ),
        {"query": f"%{q}%"}
    )
    return result.fetchall()
```

## Building a Frontend

### Using React

Here's an example of a simple React frontend for SkypeParser:

```jsx
// App.js
import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Route, Switch, Link } from 'react-router-dom';
import axios from 'axios';
import './App.css';

const API_URL = 'http://localhost:5000/api';

function ConversationList() {
  const [conversations, setConversations] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get(`${API_URL}/conversations`)
      .then(response => {
        setConversations(response.data);
        setLoading(false);
      })
      .catch(error => {
        console.error('Error fetching conversations:', error);
        setLoading(false);
      });
  }, []);

  if (loading) return <div>Loading conversations...</div>;

  return (
    <div className="conversation-list">
      <h2>Conversations</h2>
      <ul>
        {conversations.map(conversation => (
          <li key={conversation.conversation_id}>
            <Link to={`/conversations/${conversation.conversation_id}`}>
              {conversation.display_name}
            </Link>
            <span className="message-count">
              {conversation.message_count} messages
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function MessageList({ match }) {
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const conversationId = match.params.id;

  useEffect(() => {
    loadMessages();
  }, [conversationId, page]);

  const loadMessages = () => {
    axios.get(`${API_URL}/conversations/${conversationId}/messages?page=${page}`)
      .then(response => {
        if (response.data.length === 0) {
          setHasMore(false);
        } else {
          setMessages(prevMessages => [...prevMessages, ...response.data]);
        }
        setLoading(false);
      })
      .catch(error => {
        console.error('Error fetching messages:', error);
        setLoading(false);
      });
  };

  const loadMore = () => {
    setPage(prevPage => prevPage + 1);
  };

  if (loading && page === 1) return <div>Loading messages...</div>;

  return (
    <div className="message-list">
      <h2>Messages</h2>
      <div className="messages">
        {messages.map(message => (
          <div key={message.message_id} className="message">
            <div className="message-header">
              <span className="sender">{message.sender_name}</span>
              <span className="timestamp">
                {new Date(message.timestamp).toLocaleString()}
              </span>
            </div>
            <div className="message-content">{message.content}</div>
          </div>
        ))}
      </div>
      {hasMore && (
        <button onClick={loadMore} className="load-more">
          Load More
        </button>
      )}
    </div>
  );
}

function Search() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [searching, setSearching] = useState(false);

  const handleSearch = (e) => {
    e.preventDefault();
    if (!query) return;

    setSearching(true);
    axios.get(`${API_URL}/search?q=${encodeURIComponent(query)}`)
      .then(response => {
        setResults(response.data);
        setSearching(false);
      })
      .catch(error => {
        console.error('Error searching messages:', error);
        setSearching(false);
      });
  };

  return (
    <div className="search">
      <h2>Search Messages</h2>
      <form onSubmit={handleSearch}>
        <input
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Search for messages..."
        />
        <button type="submit">Search</button>
      </form>
      {searching && <div>Searching...</div>}
      <div className="search-results">
        {results.map(result => (
          <div key={result.message_id} className="search-result">
            <div className="result-header">
              <span className="conversation">{result.conversation_name}</span>
              <span className="sender">{result.sender_name}</span>
              <span className="timestamp">
                {new Date(result.timestamp).toLocaleString()}
              </span>
            </div>
            <div className="result-content">{result.content}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function App() {
  return (
    <Router>
      <div className="app">
        <header>
          <h1>SkypeParser Web App</h1>
          <nav>
            <Link to="/">Conversations</Link>
            <Link to="/search">Search</Link>
          </nav>
        </header>
        <main>
          <Switch>
            <Route exact path="/" component={ConversationList} />
            <Route path="/conversations/:id" component={MessageList} />
            <Route path="/search" component={Search} />
          </Switch>
        </main>
      </div>
    </Router>
  );
}

export default App;
```

## Using Supabase

If you're using Supabase, you can leverage its JavaScript client to build a web app:

```jsx
// App.js with Supabase
import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Route, Switch, Link } from 'react-router-dom';
import { createClient } from '@supabase/supabase-js';
import './App.css';

const supabaseUrl = 'https://your-project-id.supabase.co';
const supabaseKey = 'your-supabase-anon-key';
const supabase = createClient(supabaseUrl, supabaseKey);

function ConversationList() {
  const [conversations, setConversations] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchConversations() {
      const { data, error } = await supabase
        .from('skype_conversations')
        .select('*')
        .order('display_name');

      if (error) {
        console.error('Error fetching conversations:', error);
      } else {
        setConversations(data);
      }
      setLoading(false);
    }

    fetchConversations();
  }, []);

  if (loading) return <div>Loading conversations...</div>;

  return (
    <div className="conversation-list">
      <h2>Conversations</h2>
      <ul>
        {conversations.map(conversation => (
          <li key={conversation.conversation_id}>
            <Link to={`/conversations/${conversation.conversation_id}`}>
              {conversation.display_name}
            </Link>
            <span className="message-count">
              {conversation.message_count} messages
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function MessageList({ match }) {
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const conversationId = match.params.id;
  const messagesPerPage = 50;

  useEffect(() => {
    async function fetchMessages() {
      const from = (page - 1) * messagesPerPage;
      const to = from + messagesPerPage - 1;

      const { data, error } = await supabase
        .from('skype_messages')
        .select('*')
        .eq('conversation_id', conversationId)
        .order('timestamp')
        .range(from, to);

      if (error) {
        console.error('Error fetching messages:', error);
      } else {
        if (data.length === 0) {
          setHasMore(false);
        } else {
          setMessages(prevMessages => [...prevMessages, ...data]);
        }
      }
      setLoading(false);
    }

    fetchMessages();
  }, [conversationId, page]);

  const loadMore = () => {
    setPage(prevPage => prevPage + 1);
  };

  if (loading && page === 1) return <div>Loading messages...</div>;

  return (
    <div className="message-list">
      <h2>Messages</h2>
      <div className="messages">
        {messages.map(message => (
          <div key={message.message_id} className="message">
            <div className="message-header">
              <span className="sender">{message.sender_name}</span>
              <span className="timestamp">
                {new Date(message.timestamp).toLocaleString()}
              </span>
            </div>
            <div className="message-content">{message.content}</div>
          </div>
        ))}
      </div>
      {hasMore && (
        <button onClick={loadMore} className="load-more">
          Load More
        </button>
      )}
    </div>
  );
}

function Search() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState([]);
  const [searching, setSearching] = useState(false);

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!query) return;

    setSearching(true);

    const { data, error } = await supabase
      .from('skype_messages')
      .select(`
        *,
        skype_conversations (display_name)
      `)
      .ilike('content', `%${query}%`)
      .order('timestamp', { ascending: false })
      .limit(100);

    if (error) {
      console.error('Error searching messages:', error);
    } else {
      setResults(data.map(item => ({
        ...item,
        conversation_name: item.skype_conversations.display_name
      })));
    }
    setSearching(false);
  };

  return (
    <div className="search">
      <h2>Search Messages</h2>
      <form onSubmit={handleSearch}>
        <input
          type="text"
          value={query}
          onChange={e => setQuery(e.target.value)}
          placeholder="Search for messages..."
        />
        <button type="submit">Search</button>
      </form>
      {searching && <div>Searching...</div>}
      <div className="search-results">
        {results.map(result => (
          <div key={result.message_id} className="search-result">
            <div className="result-header">
              <span className="conversation">{result.conversation_name}</span>
              <span className="sender">{result.sender_name}</span>
              <span className="timestamp">
                {new Date(result.timestamp).toLocaleString()}
              </span>
            </div>
            <div className="result-content">{result.content}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function App() {
  return (
    <Router>
      <div className="app">
        <header>
          <h1>SkypeParser Web App</h1>
          <nav>
            <Link to="/">Conversations</Link>
            <Link to="/search">Search</Link>
          </nav>
        </header>
        <main>
          <Switch>
            <Route exact path="/" component={ConversationList} />
            <Route path="/conversations/:id" component={MessageList} />
            <Route path="/search" component={Search} />
          </Switch>
        </main>
      </div>
    </Router>
  );
}

export default App;
```

## Deployment Options

There are several options for deploying your SkypeParser web app:

### Option 1: Local Deployment

For personal use, you can run the web app locally:

```bash
# Start the backend API
cd backend
python app.py

# Start the frontend
cd frontend
npm start
```

### Option 2: Docker Deployment

You can use Docker to containerize your application:

```dockerfile
# Backend Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "app.py"]
```

```dockerfile
# Frontend Dockerfile
FROM node:14-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm install

COPY . .

RUN npm run build

FROM nginx:alpine
COPY --from=0 /app/build /usr/share/nginx/html
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
```

### Option 3: Cloud Deployment

You can deploy your web app to cloud platforms:

- **Backend**: Heroku, AWS Lambda, Google Cloud Functions, Azure Functions
- **Frontend**: Netlify, Vercel, GitHub Pages, AWS S3 + CloudFront
- **Database**: Supabase, AWS RDS, Google Cloud SQL, Azure Database for PostgreSQL

## Authentication

For multi-user web apps, you'll need to implement authentication:

### Using Supabase Auth

```jsx
import { createClient } from '@supabase/supabase-js';
import { Auth } from '@supabase/auth-ui-react';
import { ThemeSupa } from '@supabase/auth-ui-shared';

const supabaseUrl = 'https://your-project-id.supabase.co';
const supabaseKey = 'your-supabase-anon-key';
const supabase = createClient(supabaseUrl, supabaseKey);

function AuthComponent() {
  return (
    <Auth
      supabaseClient={supabase}
      appearance={{ theme: ThemeSupa }}
      providers={['google', 'github']}
    />
  );
}
```

### Using JWT with Flask

```python
from flask import Flask, request, jsonify
import jwt
import datetime
from functools import wraps

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')

        if not token:
            return jsonify({'message': 'Token is missing!'}), 401

        try:
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=["HS256"])
        except:
            return jsonify({'message': 'Token is invalid!'}), 401

        return f(*args, **kwargs)

    return decorated

@app.route('/api/login', methods=['POST'])
def login():
    auth = request.json

    if auth and auth['username'] == 'admin' and auth['password'] == 'password':
        token = jwt.encode({
            'user': auth['username'],
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24)
        }, app.config['SECRET_KEY'], algorithm="HS256")

        return jsonify({'token': token})

    return jsonify({'message': 'Invalid credentials!'}), 401

@app.route('/api/protected', methods=['GET'])
@token_required
def protected():
    return jsonify({'message': 'This is a protected endpoint!'})
```

## Advanced Features

### Real-time Updates

Using Supabase's real-time features:

```jsx
import { useEffect, useState } from 'react';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = 'https://your-project-id.supabase.co';
const supabaseKey = 'your-supabase-anon-key';
const supabase = createClient(supabaseUrl, supabaseKey);

function RealtimeMessages({ conversationId }) {
  const [messages, setMessages] = useState([]);

  useEffect(() => {
    // Fetch initial messages
    fetchMessages();

    // Subscribe to new messages
    const subscription = supabase
      .channel('public:skype_messages')
      .on('postgres_changes', {
        event: 'INSERT',
        schema: 'public',
        table: 'skype_messages',
        filter: `conversation_id=eq.${conversationId}`
      }, payload => {
        setMessages(prevMessages => [...prevMessages, payload.new]);
      })
      .subscribe();

    return () => {
      supabase.removeChannel(subscription);
    };
  }, [conversationId]);

  const fetchMessages = async () => {
    const { data } = await supabase
      .from('skype_messages')
      .select('*')
      .eq('conversation_id', conversationId)
      .order('timestamp', { ascending: false })
      .limit(50);

    setMessages(data || []);
  };

  return (
    <div>
      {messages.map(message => (
        <div key={message.id}>
          <strong>{message.sender_name}</strong>: {message.content}
        </div>
      ))}
    </div>
  );
}
```

### Message Attachments

Handling attachments with Supabase Storage:

```jsx
import { useState } from 'react';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = 'https://your-project-id.supabase.co';
const supabaseKey = 'your-supabase-anon-key';
const supabase = createClient(supabaseUrl, supabaseKey);

function MessageAttachment({ attachmentId }) {
  const [url, setUrl] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchAttachment() {
      const { data, error } = await supabase
        .from('skype_attachments')
        .select('*')
        .eq('id', attachmentId)
        .single();

      if (error || !data) {
        console.error('Error fetching attachment:', error);
        setLoading(false);
        return;
      }

      const { data: fileData, error: fileError } = await supabase
        .storage
        .from('skype-attachments')
        .download(data.storage_path);

      if (fileError) {
        console.error('Error downloading attachment:', fileError);
        setLoading(false);
        return;
      }

      const url = URL.createObjectURL(fileData);
      setUrl(url);
      setLoading(false);
    }

    fetchAttachment();

    return () => {
      if (url) URL.revokeObjectURL(url);
    };
  }, [attachmentId]);

  if (loading) return <div>Loading attachment...</div>;
  if (!url) return <div>Attachment not found</div>;

  return (
    <div className="attachment">
      <img src={url} alt="Attachment" />
    </div>
  );
}
```

## Troubleshooting

### Common Issues

1. **CORS Errors**: Ensure your backend has CORS headers properly configured.
2. **Database Connection Issues**: Check your database connection string and credentials.
3. **Authentication Problems**: Verify your authentication flow and token handling.
4. **Performance Issues**: Implement pagination and optimize database queries for large datasets.

### Performance Optimization

For large Skype datasets, consider these optimizations:

1. **Pagination**: Always use pagination for message lists.
2. **Indexing**: Create indexes on frequently queried columns:
   ```sql
   CREATE INDEX idx_messages_conversation_id ON skype_messages(conversation_id);
   CREATE INDEX idx_messages_timestamp ON skype_messages(timestamp);
   CREATE INDEX idx_messages_content_gin ON skype_messages USING gin(to_tsvector('english', content));
   ```
3. **Caching**: Implement caching for frequently accessed data.
4. **Lazy Loading**: Load images and attachments only when needed.

## Next Steps

Now that you understand how to build a web app with SkypeParser, you can:

- Explore the [Data Analysis Guide](data-analysis.md) for adding analytics to your web app
- Learn about [Visualization](visualization.md) for creating charts and graphs
- Check out the [Supabase Integration Guide](database/supabase.md) for more advanced Supabase features