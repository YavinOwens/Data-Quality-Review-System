import streamlit as st
import pandas as pd
import numpy as np
from mitosheet import sheet
import graphviz
from datetime import datetime
import json
import psycopg2
from sqlalchemy import create_engine, MetaData, inspect
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from streamlit_react_flow import react_flow
from streamlit_option_menu import option_menu

# Must be the first Streamlit command
st.set_page_config(layout="wide")

# Add custom CSS for React Flow
st.markdown("""
<style>
.react-flow {
    height: 600px;
    width: 100%;
}
.react-flow__node {
        padding: 10px;
    border-radius: 3px;
    width: 150px;
    font-size: 12px;
    color: #222;
    text-align: center;
    border-width: 1px;
    border-style: solid;
    background: #fff;
    border-color: #1a192b;
}
.react-flow__edge {
    stroke: #222;
}
.react-flow__edge-path {
    stroke-width: 2;
}
.react-flow__edge-text {
    font-size: 12px;
}
.react-flow__edge.animated path {
    stroke-dasharray: 5;
    animation: dashdraw 0.5s linear infinite;
}
@keyframes dashdraw {
    from {
        stroke-dashoffset: 10;
    }
    }
</style>
""", unsafe_allow_html=True)

def init_connection():
    """Initialize database connection."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="mysecretpassword",
            port="5432"
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return None

def get_tables():
    """Get all tables from the database."""
    try:
        conn = init_connection()
        if not conn:
            return pd.DataFrame()
        
        query = """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to get tables: {str(e)}")
        return pd.DataFrame()

def get_table_stats(table_name):
    """Get table statistics."""
    try:
        conn = init_connection()
        if not conn:
            return pd.DataFrame({'total_rows': [0], 'unique_rows': [0]})
        
        # Get all column names
        columns_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
            ORDER BY ordinal_position;
            """
        columns_df = pd.read_sql(columns_query, conn, params=[table_name])
        columns = columns_df['column_name'].tolist()
        
        # Create concatenated columns string for distinct count
        columns_concat = "CONCAT(" + ", '|', ".join(columns) + ")"
        
        # Get statistics
        stats_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT {columns_concat}) as unique_rows
            FROM {table_name};
            """
        df = pd.read_sql(stats_query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to get table statistics: {str(e)}")
        return pd.DataFrame({'total_rows': [0], 'unique_rows': [0]})

def get_table_schema(table_name):
    """Get table schema."""
    try:
        conn = init_connection()
        if not conn:
            return pd.DataFrame()
        
        query = """
    SELECT 
        column_name,
        data_type,
        character_maximum_length,
                is_nullable,
                column_default
    FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = %s
    ORDER BY ordinal_position;
    """
        df = pd.read_sql(query, conn, params=[table_name])
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to get table schema: {str(e)}")
        return pd.DataFrame()

def get_table_preview(table_name):
    """Get table preview data."""
    try:
        conn = init_connection()
        if not conn:
            return pd.DataFrame()
        
        query = f"SELECT * FROM {table_name} LIMIT 5"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to get table preview: {str(e)}")
        return pd.DataFrame()

def get_foreign_keys():
    """Get all foreign key relationships."""
    try:
        conn = init_connection()
        if not conn:
            return []
        
        query = """
    SELECT 
                tc.table_schema, 
                tc.constraint_name, 
                tc.table_name as table, 
                kcu.column_name as column, 
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS referenced_table,
                ccu.column_name AS referenced_column 
            FROM 
                information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                  AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                  AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY';
            """
        df = pd.read_sql(query, conn)
        conn.close()
        return df.to_dict('records')
    except Exception as e:
        st.error(f"Failed to get foreign keys: {str(e)}")
        return []

def generate_erd(selected_tables):
    """Generate ERD for multiple selected tables."""
    try:
        # Create a new directed graph
        dot = graphviz.Digraph(comment='Entity Relationship Diagram')
        dot.attr(rankdir='LR')
        dot.attr('node', shape='plaintext')
        
        # Keep track of added tables to avoid duplicates
        added_tables = set()
        
        # Process each selected table
        for table_name in selected_tables:
            if table_name in added_tables:
                continue
                
            # Get schema for the table
            schema = get_table_schema(table_name)
            
            # Create HTML-like label for the table
            table_label = f'''<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                <TR><TD PORT="title" BGCOLOR="#E8F4F8"><B>{table_name}</B></TD></TR>'''
            
            # Add columns
            for _, row in schema.iterrows():
                col_name = row['column_name']
                data_type = row['data_type']
                nullable = "NULL" if row['is_nullable'] == "YES" else "NOT NULL"
                table_label += f'<TR><TD PORT="{col_name}" ALIGN="LEFT">{col_name} {data_type} {nullable}</TD></TR>'
            
            table_label += '</TABLE>>'
            dot.node(table_name, table_label)
            added_tables.add(table_name)
            
            # Get and add related tables through foreign keys
            fks = get_foreign_keys()
            
            for fk in fks:
                related_table = fk['referenced_table']
                
                # Only add the related table if it's in our selection
                if related_table in selected_tables:
                    if related_table not in added_tables:
                        # Get schema for related table
                        related_schema = get_table_schema(related_table)
                        
                        # Create HTML-like label for related table
                        related_label = f'''<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
                            <TR><TD PORT="title" BGCOLOR="#E8F4F8"><B>{related_table}</B></TD></TR>'''
                        
                        # Add columns for related table
                        for _, row in related_schema.iterrows():
                            col_name = row['column_name']
                            data_type = row['data_type']
                            nullable = "NULL" if row['is_nullable'] == "YES" else "NOT NULL"
                            related_label += f'<TR><TD PORT="{col_name}" ALIGN="LEFT">{col_name} {data_type} {nullable}</TD></TR>'
                        
                        related_label += '</TABLE>>'
                        dot.node(related_table, related_label)
                        added_tables.add(related_table)
                    
                    # Add edge for the relationship
                    dot.edge(f'{table_name}:{fk["column"]}', f'{related_table}:{fk["referenced_column"]}')
        
        return dot
    except Exception as e:
        st.error(f"Failed to generate ERD: {str(e)}")
        return None

def get_joined_data(selected_tables, foreign_keys):
    """Get joined data for selected tables based on their relationships."""
    try:
        conn = init_connection()
        if not conn or len(selected_tables) < 2:
            return pd.DataFrame()
        
        # Start with the first table
        base_table = selected_tables[0]
        joins = []
        seen_tables = {base_table}
        
        # Build JOIN clauses based on foreign key relationships
        for fk in foreign_keys:
            table1 = fk['table']
            table2 = fk['referenced_table']
            col1 = fk['column']
            col2 = fk['referenced_column']
            
            if table1 in selected_tables and table2 in selected_tables:
                if table1 not in seen_tables and table2 in seen_tables:
                    joins.append(f"LEFT JOIN {table1} ON {table2}.{col2} = {table1}.{col1}")
                    seen_tables.add(table1)
                elif table2 not in seen_tables and table1 in seen_tables:
                    joins.append(f"LEFT JOIN {table2} ON {table1}.{col1} = {table2}.{col2}")
                    seen_tables.add(table2)
        
        if not joins:
            return pd.DataFrame()
        
        # Build column list for each table
        columns = []
        for table in selected_tables:
            schema = get_table_schema(table)
            columns.extend([f"{table}.{col} as {table}_{col}" for col in schema['column_name']])
        
        # Construct and execute query
        query = f"""
            SELECT {', '.join(columns)}
            FROM {base_table}
            {' '.join(joins)}
            LIMIT 1000;
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Failed to get joined data: {str(e)}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def human_format(num):
    """Format numbers in human readable format."""
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '%.1f%s' % (num, ['', 'K', 'M', 'B', 'T'][magnitude])

def execute_query(query, conn):
    """Execute a SQL query and return results as a DataFrame."""
    try:
        df = pd.read_sql_query(query, conn)
        return df, None
    except Exception as e:
        return None, str(e)

def get_table_names():
    """Get list of table names from the database."""
    try:
        tables_df = get_tables()
        return tables_df['table_name'].tolist()
    except Exception as e:
        st.error(f"Failed to get table names: {str(e)}")
        return []

def main():
    conn = None
    try:
        conn = init_connection()
        if not conn:
            st.error("Failed to connect to database")
            return

        # Navigation menu at the top
        selected = option_menu(
            menu_title=None,
            options=["Preview Data", "Schema", "Statistics", "ERD", "Query", "Architecture"],
            icons=["table", "list-columns", "graph-up", "diagram-3", "code-square", "boxes"],
            menu_icon="cast",
            default_index=0,
            orientation="horizontal",
            styles={
                "container": {"padding": "0!important", "background-color": "#f8f9fa", "margin-bottom": "20px"},
                "icon": {"color": "#666", "font-size": "16px"},
                "nav-link": {
                    "font-size": "14px",
                    "text-align": "center",
                    "margin": "0px",
                    "--hover-color": "#eee",
                    "color": "#666",
                },
                "nav-link-selected": {"background-color": "#e8f4f8", "color": "#333"},
            }
        )
        
        # Search box
        st.markdown('<div class="search-container">', unsafe_allow_html=True)
        search_term = st.text_input("Search tables...", 
                                   placeholder="Search by table name...", 
                                   label_visibility="collapsed")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Get and filter tables
        tables_df = get_tables()
        if search_term:
            tables_df = tables_df[tables_df['table_name'].str.contains(search_term, case=False)]
        
        # Statistics Dashboard - Horizontal Layout
        total_tables = len(tables_df[tables_df['table_type'] == 'BASE TABLE'])
        total_views = len(tables_df[tables_df['table_type'] == 'VIEW'])
        total_rows = sum([get_table_stats(table)['total_rows'].iloc[0] 
                         for table in tables_df['table_name']])
        
        st.markdown('''
        <div class="stats-container">
            <div class="stats-dashboard">
                <div class="stat-card">
                    <p class="stat-value">{}</p>
                    <p class="stat-label">Base Tables</p>
                </div>
                <div class="stat-card">
                    <p class="stat-value">{}</p>
                    <p class="stat-label">Views</p>
                </div>
                <div class="stat-card">
                    <p class="stat-value">{}</p>
                    <p class="stat-label">Total Rows</p>
                </div>
            </div>
        </div>
        '''.format(total_tables, total_views, human_format(total_rows)), unsafe_allow_html=True)
        
        # Display tables with their respective views
        if selected in ["Preview Data", "Schema", "Statistics"]:
            for _, row in tables_df.iterrows():
                table_name = row['table_name']
                stats = get_table_stats(table_name)
                schema = get_table_schema(table_name)
                
                # Display table info with consistent light blue background
                st.markdown(f'''
                <div class="table-info-header">
                    <div class="table-name">{table_name}</div>
                    <div class="table-metrics">
                        <span>{human_format(stats['total_rows'].iloc[0])} Total Rows</span>
                        <span>{len(schema)} Columns</span>
                    </div>
                    <div class="table-metadata">
                        <span>Owner: postgres</span>
                        <span>Created: {datetime.now().strftime('%Y-%m-%d')}</span>
                    </div>
                </div>
                ''', unsafe_allow_html=True)
                
                if selected == "Preview Data":
                    with st.expander("Preview", expanded=False):
                        st.dataframe(get_table_preview(table_name))
                elif selected == "Schema":
                    with st.expander("Schema", expanded=False):
                        st.dataframe(schema)
                else:  # Statistics view
                    with st.expander("Statistics", expanded=False):
                        st.dataframe(stats)
        
        elif selected == "Query":
            st.header("SQL Query Interface")
            
            # Query input with syntax highlighting
            query = st.text_area("Enter your SQL query:", height=150)
            
            if st.button("Run Query"):
                if query:
                    try:
                        df, error = execute_query(query, conn)
                        if error:
                            st.error(f"Error executing query: {error}")
                        else:
                            st.success("Query executed successfully!")
                            st.markdown('''
                            <div class="query-results">
                                <h3>Query Results</h3>
                            </div>
                            ''', unsafe_allow_html=True)
                            st.dataframe(df)
                    except Exception as e:
                        st.error(f"Error executing query: {str(e)}")
                else:
                    st.warning("Please enter a query to execute")
        
        elif selected == "ERD":
            st.header("Entity Relationship Diagram")
            
            try:
                # Table selection
                tables = get_table_names()
                selected_tables = st.multiselect("Select tables to view relationships", tables)
                
                if selected_tables:
                    # ERD zoom control
                    zoom_level = st.slider("Zoom Level", 0.5, 2.0, 1.0, 0.1, key="erd_zoom")
                    
                    # Generate and display ERD using Graphviz
                    dot = generate_erd(selected_tables)
                    if dot:
                        dot.attr(rankdir='LR', size=str(11*zoom_level))
                        st.graphviz_chart(dot)
                    
                    # Show joined data if multiple tables selected
                    if len(selected_tables) > 1:
                        with st.expander("View Joined Data"):
                            df = get_joined_data(selected_tables, get_foreign_keys())
                            if df is not None and not df.empty:
                                gb = GridOptionsBuilder.from_dataframe(df)
                                gb.configure_default_column(groupable=True, value=True, enableRowGroup=True,
                                                         aggFunc='sum', editable=False)
                                gb.configure_selection('multiple', use_checkbox=True)
                                gb.configure_grid_options(domLayout='normal')
                                gridOptions = gb.build()
                                
                                grid_response = AgGrid(
                                    df,
                                    gridOptions=gridOptions,
                                    height=400,
                                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                                    update_mode=GridUpdateMode.MODEL_CHANGED,
                                    fit_columns_on_grid_load=True
                                )
                                
                                selected_rows = grid_response['selected_rows']
                                if selected_rows:
                                    st.write('Selected rows:')
                                    st.dataframe(pd.DataFrame(selected_rows))
            except Exception as e:
                st.error(f"Error in ERD view: {str(e)}")
        
        elif selected == "Architecture":
            st.header("Database Architecture")
            
            try:
                # Get tables and relationships
                tables = get_table_names()
                fks = get_foreign_keys()
                
                # Calculate layout positions
                num_tables = len(tables)
                columns = min(4, num_tables)  # Maximum 4 tables per row
                rows = (num_tables + columns - 1) // columns
                spacing_x = 300  # Horizontal spacing between nodes
                spacing_y = 200  # Vertical spacing between rows
                
                # Create nodes for each table with calculated positions
                elements = []
                for i, table in enumerate(tables):
                    row = i // columns
                    col = i % columns
                    elements.append({
                        'id': table,
                        'data': {'label': table},
                        'position': {
                            'x': col * spacing_x + 100,  # Add initial offset
                            'y': row * spacing_y + 100   # Add initial offset
                        },
                        'style': {
                            'background': '#e8f4f8',
                            'width': 180,
                            'border': '1px solid #666',
                            'borderRadius': '4px',
                            'padding': '10px',
                            'fontSize': '14px'
                        }
                    })
                
                # Add edges for relationships with improved styling
                for fk in fks:
                    elements.append({
                        'id': f"{fk['table']}-{fk['referenced_table']}",
                        'source': fk['table'],
                        'target': fk['referenced_table'],
                        'animated': True,
                        'label': f"{fk['column']} → {fk['referenced_column']}",
                        'style': {
                            'stroke': '#666',
                            'strokeWidth': 2
                        },
                        'labelStyle': {
                            'fontSize': '12px',
                            'fill': '#666'
                        },
                        'type': 'smoothstep'  # Use smooth edges
                    })
                
                # Set flow styles
                flow_styles = {
                    'height': f'{max(600, rows * spacing_y + 200)}px',
                    'width': '100%',
                    'background': '#f8f9fa',
                    'border': '1px solid #ddd',
                    'borderRadius': '4px'
                }
                
                # Render the flow diagram
                react_flow(
                    "db_architecture",
                    elements=elements,
                    flow_styles=flow_styles,
                    fit_view=True,
                    node_dimensions_change_mode='default',
                    edge_type='smoothstep',
                    min_zoom=0.5,
                    max_zoom=2
                )
            except Exception as e:
                st.error(f"Error in Architecture view: {str(e)}")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
