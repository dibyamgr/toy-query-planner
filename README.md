# 📝 Toy Query Planner: A Rule-Based Optimization Engine

A simplified query optimization system that demonstrates core database optimization principles through heuristic-based transformations.

## Project Overview

This project is an educational Toy Query Planner designed to demonstrate the fundamental pipeline of database query optimization. It takes high-level SQL queries, translates them into a formal Relational Algebra (RA) Tree, applies a heuristic optimization rule, and executes the resulting plan against an in-memory data catalog.

## Team Members

- Tanzim Rahman (202581528)
- Dibya Rani Saru Magar (202487865)
- Priyanka Saha (202387432)
- Durga Khanal (202488114)

**Institution:** Memorial University of Newfoundland  
**Program:** Master's in Software Engineering  
**Course:** COMP 4754

## Architecture

### Phase 1: Parsing
- Parses SQL SELECT queries with FROM, WHERE, and JOIN clauses
- Constructs Abstract Syntax Trees (AST)
- Transforms AST into relational algebra operator trees

### Phase 2: Optimization
Implements heuristic-based optimization rules:
- **Predicate Pushdown (σ):** Applies filtering early to reduce data volume
- **Projection Pushdown (π):** Eliminates unnecessary columns early
- **Join Ordering:** Optimizes join sequence for efficiency

### Phase 3: Execution
- Converts logical plans into physical execution plans
- Measures performance metrics (execution time, tuple counts)
- Compares optimized vs. non-optimized plans

## Key Features

- ✅ Lightweight parser built with Python standard libraries
- ✅ Visual representation of query transformation steps
- ✅ Quantifiable performance improvements through heuristics
- ✅ Educational focus on fundamental optimization principles

## Technology Stack

- **Backend Framework** : Flask (Python)

- **Core Logic** : Pure Python modules for parsing and planning.

- **Frontend** : Simple HTML and JavaScript (for interactive query submission and plan visualization).

- **Data Model** : In-memory Data Catalog for quick, controlled execution.
- 
- **Data Format:** CSV/JSON for test datasets

## Setup and Usage

To run the Toy Query Planner locally, follow these steps (assuming Python 3.9+ is installed):

### Prerequisites

1. Clone the Repository: git@github.com:dibyamgr/toy-query-planner.git

```
git clone git@github.com:dibyamgr/toy-query-planner.git
cd toy-query-planner
```

2. Create and Activate Virtual Environment:
```
python -m venv venv
source venv/bin/activate  # On Windows, use: .\venv\Scripts\activate
```

3. Install Dependencies:
   
```
pip install Flask
```

## Running the Application

1. Start the Flask Serve
```
python app.py
```

2. Access the Frontend: Open your web browser and navigate to
```http://127.0.0.1:5000/```

You can now submit SQL queries (e.g., ```SELECT name, age FROM users WHERE age > 25 LIMIT 10```) and observe the difference between the Initial Logical Plan and the Optimized Logical Plan.

## Expected Outcomes

- Functional query optimizer demonstrating measurable performance improvements
- Empirical evidence showing optimization effectiveness (targeting 20%+ improvement)
- Clear visualization of how optimization transforms query execution

## Research Foundation

Based on foundational work in query optimization including:
- Predicate pushdown techniques (Drozd et al., 2013)
- Heuristic optimization validation (Indrayana et al., 2018)
- Classical optimization theory (Jarke & Koch, 1984; Selinger et al., 1979)

*This project is part of academic coursework and is intended for educational purposes.*