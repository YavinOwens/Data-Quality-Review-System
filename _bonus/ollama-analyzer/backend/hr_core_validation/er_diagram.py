"""
Generate ER diagrams using sadisplay.
"""
import sadisplay
from .schemas.workers import WorkersSchema
from .schemas.assignments import AssignmentsSchema
from .schemas.communications import CommunicationsSchema
from .schemas.addresses import AddressesSchema

def generate_er_diagram():
    """
    Generate an ER diagram for the HR Core data model using sadisplay.
    Returns the dot format string representation of the diagram.
    """
    # Collect all schema models
    models = [
        WorkersSchema,
        AssignmentsSchema,
        CommunicationsSchema,
        AddressesSchema
    ]
    
    # Generate the ER diagram description
    desc = sadisplay.describe(models)
    
    # Generate DOT format
    dot = sadisplay.dot(desc)
    
    return dot

def save_er_diagram(output_path='er_diagram.dot'):
    """
    Generate and save the ER diagram to a DOT file.
    Args:
        output_path (str): Path where to save the DOT file
    """
    dot = generate_er_diagram()
    with open(output_path, 'w') as f:
        f.write(dot) 