import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from streamlit_option_menu import option_menu
from streamlit_elements import elements, dashboard, mui, html, nivo
import os
import sys
from subprocess import run, PIPE

# Try importing graphviz and handle potential import errors
try:
    import graphviz
    GRAPHVIZ_AVAILABLE = True
except ImportError:
    GRAPHVIZ_AVAILABLE = False

# Set page configuration to wide mode
st.set_page_config(layout="wide")

# Custom CSS for dashboard
st.markdown("""
<style>
    .element-container {
        background-color: white;
        border-radius: 10px;
        padding: 10px;
        margin: 10px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# Set up the PostgreSQL connection
def init_connection():
    return create_engine('postgresql://postgres:mysecretpassword@localhost:5432/postgres')

# Function to get all table names
def get_tables(engine):
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    """
    with engine.connect() as conn:
        tables = pd.read_sql(query, conn)
    return tables['table_name'].tolist()

# Function to get table data
def get_table_data(engine, table_name):
    query = f"SELECT * FROM {table_name}"
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# Function to get table schema
def get_table_schema(engine, table_name):
    query = f"""
    SELECT 
        column_name,
        data_type,
        character_maximum_length,
        column_default,
        is_nullable
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# Function to get table statistics
def get_table_stats(engine, table_name):
    query = f"""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT *) as unique_rows
    FROM {table_name}
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

# Function to get column statistics
def get_column_stats(engine, table_name, column_name):
    query = f"""
    SELECT 
        '{column_name}' as field,
        COUNT(*) as total,
        COUNT(DISTINCT {column_name}) as unique_values,
        COUNT(*) - COUNT({column_name}) as null_count
    FROM {table_name}
    """
    with engine.connect() as conn:
        return pd.read_sql(query, conn)

def check_graphviz_installation():
    """Check if Graphviz is properly installed in the system."""
    try:
        # Try running dot -V to check if Graphviz is installed
        result = run(['dot', '-V'], stdout=PIPE, stderr=PIPE, text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

# Function to generate ERD diagram
def generate_erd(engine, output_dir="static/images"):
    """
    Generate an ERD diagram for the database schema.
    
    Args:
        engine: SQLAlchemy engine
        output_dir: Directory to save the generated files
        
    Returns:
        str: Path to the generated PNG file
    """
    try:
        # Check if Python graphviz package is available
        if not GRAPHVIZ_AVAILABLE:
            st.error("""
                Python graphviz package is not installed. Please install it using:
                ```
                pip install python-graphviz
                ```
            """)
            return None
            
        # Check if system Graphviz is installed
        if not check_graphviz_installation():
            st.error("""
                System-level Graphviz is not installed. Please install it using:
                
                For macOS:
                ```
                brew install graphviz
                ```
                
                For Ubuntu/Debian:
                ```
                sudo apt-get install graphviz
                ```
                
                For Windows:
                Download from https://graphviz.org/download/
            """)
            return None
            
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Create a new Graphviz graph
        dot = graphviz.Digraph('erd')
        dot.attr(rankdir='LR')
        
        # Set default node attributes
        dot.attr('node', shape='record', style='filled', fillcolor='lightblue')
        
        # Add tables
        tables = get_tables(engine)
        for table in tables:
            schema = get_table_schema(engine, table)
            columns = [f"{row['column_name']} : {row['data_type']}" for _, row in schema.iterrows()]
            table_label = f"{table}|{{'|'.join(columns)}}"
            dot.node(table, table_label)
        
        # Add relationships
        fk_query = """
        SELECT
            tc.table_name as source_table,
            kcu.column_name as source_column,
            ccu.table_name AS target_table,
            ccu.column_name AS target_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY';
        """
        with engine.connect() as conn:
            fk_results = pd.read_sql(fk_query, conn)
        
        # Add edges for foreign key relationships
        for _, fk in fk_results.iterrows():
            dot.edge(
                fk["source_table"],
                fk["target_table"],
                _attributes={'arrowhead': 'crow', 'arrowtail': 'none'}
            )
        
        try:
            # Save the graph
            output_path = os.path.join(output_dir, "schema_erd")
            dot.render(output_path, format='png', cleanup=True)
            return output_path + '.png'
        except graphviz.ExecutableNotFound:
            st.error("""
                Graphviz executable not found in PATH. Please ensure Graphviz is properly installed 
                and the 'dot' executable is in your system PATH.
            """)
            return None
        
    except Exception as e:
        st.error(f"Error generating ERD: {str(e)}")
        return None

# Streamlit app
st.title('PostgreSQL Database Explorer')

try:
    # Initialize connection
    engine = init_connection()
    
    # Horizontal menu
    selected = option_menu(
        menu_title=None,
        options=["Dashboard", "Tables", "Schema", "Query", "Analytics"],
        icons=["grid", "table", "list-columns", "search", "graph-up"],
        menu_icon="cast",
        default_index=0,
        orientation="horizontal",
    )
    
    # Get list of tables
    tables = get_tables(engine)
    
    if tables:
        # Create table selector
        selected_table = st.selectbox('Select a table:', tables)
        
        if selected_table:
            if selected == "Dashboard":
                # Dashboard layout
                with elements("dashboard"):
                    layout = [
                        dashboard.Item("table_data", 0, 0, 6, 4),
                        dashboard.Item("table_schema", 6, 0, 6, 4),
                        dashboard.Item("table_stats", 0, 4, 12, 2),
                    ]
                    
                    with dashboard.Grid(layout):
                        with mui.Card(key="table_data", sx={"height": "100%"}):
                            mui.CardHeader(title="Table Data Preview")
                            with mui.CardContent():
                                data = get_table_data(engine, selected_table).head()
                                st.dataframe(data, use_container_width=True)
                        
                        with mui.Card(key="table_schema", sx={"height": "100%"}):
                            mui.CardHeader(title="Table Schema")
                            with mui.CardContent():
                                schema = get_table_schema(engine, selected_table)
                                st.dataframe(schema, use_container_width=True)
                        
                        with mui.Card(key="table_stats", sx={"height": "100%"}):
                            mui.CardHeader(title="Table Statistics")
                            with mui.CardContent():
                                stats = get_table_stats(engine, selected_table)
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Total Rows", stats['total_rows'].iloc[0])
                                with col2:
                                    st.metric("Unique Rows", stats['unique_rows'].iloc[0])
            
            elif selected == "Analytics":
                # Get schema to identify columns
                schema = get_table_schema(engine, selected_table)
                columns = schema['column_name'].tolist()
                
                with elements("analytics"):
                    # Layout for analytics dashboard
                    layout = [
                        dashboard.Item("column_stats", 0, 0, 12, 3),
                        dashboard.Item("data_completeness", 0, 3, 6, 4),
                        dashboard.Item("value_distribution", 6, 3, 6, 4),
                    ]
                    
                    with dashboard.Grid(layout):
                        # Column Statistics Card with Nivo Bar Chart
                        with mui.Card(key="column_stats", sx={"height": "100%"}):
                            mui.CardHeader(title="Column Statistics")
                            with mui.CardContent():
                                # Get stats for each column
                                stats_data = []
                                for col in columns[:5]:  # Limit to first 5 columns for performance
                                    stats = get_column_stats(engine, selected_table, col)
                                    stats_data.append({
                                        "column": col,
                                        "total": int(stats['total'].iloc[0]),
                                        "unique": int(stats['unique_values'].iloc[0]),
                                        "nulls": int(stats['null_count'].iloc[0])
                                    })
                                
                                # Create Nivo bar chart
                                with mui.Box(sx={"height": 200}):
                                    nivo.Bar(
                                        data=stats_data,
                                        keys=["total", "unique", "nulls"],
                                        indexBy="column",
                                        margin={"top": 50, "right": 130, "bottom": 50, "left": 60},
                                        padding=0.3,
                                        valueScale={"type": "linear"},
                                        indexScale={"type": "band", "round": True},
                                        colors={"scheme": "nivo"},
                                        borderColor={"from": "color", "modifiers": [["darker", 1.6]]},
                                        axisTop=None,
                                        axisRight=None,
                                        axisBottom={
                                            "tickSize": 5,
                                            "tickPadding": 5,
                                            "tickRotation": -45,
                                            "legend": "Column",
                                            "legendPosition": "middle",
                                            "legendOffset": 40
                                        },
                                        axisLeft={
                                            "tickSize": 5,
                                            "tickPadding": 5,
                                            "tickRotation": 0,
                                            "legend": "Count",
                                            "legendPosition": "middle",
                                            "legendOffset": -40
                                        },
                                        labelSkipWidth=12,
                                        labelSkipHeight=12,
                                        legends=[
                                            {
                                                "dataFrom": "keys",
                                                "anchor": "bottom-right",
                                                "direction": "column",
                                                "justify": False,
                                                "translateX": 120,
                                                "translateY": 0,
                                                "itemsSpacing": 2,
                                                "itemWidth": 100,
                                                "itemHeight": 20,
                                                "itemDirection": "left-to-right",
                                                "itemOpacity": 0.85,
                                                "symbolSize": 20,
                                                "effects": [
                                                    {
                                                        "on": "hover",
                                                        "style": {
                                                            "itemOpacity": 1
                                                        }
                                                    }
                                                ]
                                            }
                                        ]
                                    )
                        
                        # Data Completeness Card with Nivo Pie Chart
                        with mui.Card(key="data_completeness", sx={"height": "100%"}):
                            mui.CardHeader(title="Data Completeness")
                            with mui.CardContent():
                                # Calculate completeness for first column
                                if columns:
                                    stats = get_column_stats(engine, selected_table, columns[0])
                                    total = int(stats['total'].iloc[0])
                                    nulls = int(stats['null_count'].iloc[0])
                                    complete = total - nulls
                                    
                                    pie_data = [
                                        {"id": "Complete", "label": "Complete", "value": complete},
                                        {"id": "Missing", "label": "Missing", "value": nulls}
                                    ]
                                    
                                    with mui.Box(sx={"height": 300}):
                                        nivo.Pie(
                                            data=pie_data,
                                            margin={"top": 40, "right": 80, "bottom": 80, "left": 80},
                                            innerRadius=0.5,
                                            padAngle=0.7,
                                            cornerRadius=3,
                                            activeOuterRadiusOffset=8,
                                            colors={"scheme": "nivo"},
                                            borderWidth=1,
                                            borderColor={
                                                "from": "color",
                                                "modifiers": [["darker", 0.2]]
                                            },
                                            arcLinkLabelsSkipAngle=10,
                                            arcLinkLabelsTextColor="#333333",
                                            arcLinkLabelsThickness=2,
                                            arcLinkLabelsColor={"from": "color"},
                                            arcLabelsSkipAngle=10,
                                            arcLabelsTextColor={
                                                "from": "color",
                                                "modifiers": [["darker", 2]]
                                            },
                                            legends=[
                                                {
                                                    "anchor": "bottom",
                                                    "direction": "row",
                                                    "justify": False,
                                                    "translateX": 0,
                                                    "translateY": 56,
                                                    "itemsSpacing": 0,
                                                    "itemWidth": 100,
                                                    "itemHeight": 18,
                                                    "itemTextColor": "#999",
                                                    "itemDirection": "left-to-right",
                                                    "itemOpacity": 1,
                                                    "symbolSize": 18,
                                                    "symbolShape": "circle",
                                                    "effects": [
                                                        {
                                                            "on": "hover",
                                                            "style": {
                                                                "itemTextColor": "#000"
                                                            }
                                                        }
                                                    ]
                                                }
                                            ]
                                        )
                        
                        # Value Distribution Card with Nivo Line Chart
                        with mui.Card(key="value_distribution", sx={"height": "100%"}):
                            mui.CardHeader(title="Value Distribution Over Time")
                            with mui.CardContent():
                                st.info("Select a datetime column to view distribution over time")
                                
            elif selected == "Tables":
                # Display table data
                st.subheader(f'Data from table: {selected_table}')
                data = get_table_data(engine, selected_table)
                st.dataframe(data, use_container_width=True)
                st.write(f'Total rows: {len(data)}')
                
            elif selected == "Schema":
                # Schema layout with ERD
                with elements("schema"):
                    layout = [
                        dashboard.Item("table_schema", 0, 0, 6, 6),
                        dashboard.Item("erd_diagram", 6, 0, 6, 6),
                    ]
                    
                    with dashboard.Grid(layout):
                        with mui.Card(key="table_schema", sx={"height": "100%"}):
                            mui.CardHeader(title="Table Schema")
                            with mui.CardContent():
                                schema = get_table_schema(engine, selected_table)
                                st.dataframe(schema, use_container_width=True)
                        
                        with mui.Card(key="erd_diagram", sx={"height": "100%"}):
                            mui.CardHeader(title="Entity-Relationship Diagram")
                            with mui.CardContent():
                                # Generate ERD button
                                if st.button("Generate ERD"):
                                    with st.spinner("Generating ERD diagram..."):
                                        erd_path = generate_erd(engine)
                                        if erd_path and os.path.exists(erd_path):
                                            st.image(erd_path, use_column_width=True)
                                        else:
                                            st.error("Failed to generate ERD diagram")
            
            elif selected == "Query":
                # Custom query interface
                st.subheader('Custom SQL Query')
                default_query = f"SELECT * FROM {selected_table} LIMIT 5;"
                query = st.text_area("Enter your SQL query:", value=default_query, height=100)
                
                if st.button('Run Query'):
                    try:
                        with engine.connect() as conn:
                            result = pd.read_sql(query, conn)
                            st.dataframe(result, use_container_width=True)
                            st.write(f'Results: {len(result)} rows')
                    except Exception as e:
                        st.error(f'Query error: {str(e)}')
    else:
        st.warning('No tables found in the database.')
        
except Exception as e:
    st.error(f'Error connecting to database: {str(e)}')
