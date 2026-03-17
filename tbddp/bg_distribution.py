import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import FancyBboxPatch, BoxStyle
import matplotlib.font_manager as fm

def create_bg_distribution_chart(values, title="BG Distribution"):
    """
    Creates a blood glucose distribution chart similar to the provided image.
    
    Parameters:
    values (list): Array of 5 numbers representing percentages for each BG range
    title (str): Chart title
    """
    
    # Register the font
    font_path = '/Library/Fonts/BasisGrotesquePro-Medium.otf'
    fm.fontManager.addfont(font_path)

    # Get the font name
    prop = fm.FontProperties(fname=font_path)
    font_name = prop.get_name()

    # Ensure we have exactly 5 values
    if len(values) != 5:
        raise ValueError("Array must contain exactly 5 values")
    
    # BG range labels and colors (from top to bottom as shown in image)
    labels = ['>250 mg/dL', '180-250 mg/dL', '70-180 mg/dL', '54-70 mg/dL', '<54 mg/dL']
    colors = ['#8C65D6', '#ba9ae7', '#76d4a6', '#FF8B7C', '#FB5951']  # Purple, light purple, green, amber, red
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(8, 4))
    
    # Create grey background bars extending to 100% with rounded corners
    y_positions = np.arange(len(values))
    bar_height = 0.6
    
    # Create rounded background bars
    for i, y_pos in enumerate(y_positions):
        background_rect = FancyBboxPatch(
            (0, y_pos - .15/2 ), 100, .1,
            boxstyle="Round,pad=0.2", 
            facecolor='#E5E7EB', alpha=0.8, edgecolor='none', mutation_aspect=0.2
        )
        ax.add_patch(background_rect)
    
    # Create rounded colored bars on top
    bars = []
    for i, (y_pos, value) in enumerate(zip(y_positions, values)):
        colored_rect = FancyBboxPatch(
            (0, y_pos - bar_height/2), value, bar_height,
            boxstyle="Round,pad=0.6",
            facecolor=colors[i],alpha=1, edgecolor='none', mutation_aspect=0.2
        )
        ax.add_patch(colored_rect)
        bars.append(colored_rect)
    
    # Customize the chart
    ax.set_yticks([])  # Remove y-tick marks
    ax.set_yticklabels([])  # Remove y-tick labels
    ax.set_xlabel('')
    ax.set_title(title, fontsize=24, color="#465367", fontfamily=font_name)
    
    # Remove spines and ticks
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.tick_params(left=False, bottom=False)
    ax.set_xticks([])
    
    # Add percentage labels and range labels to the right of the bars
    for i, (bar, value, y_pos) in enumerate(zip(bars, values, y_positions)):
        # Position percentage text at the end of the background bars
        value_x = 120 + max(values) * 0.02
        ax.text(value_x, y_pos, 
            f'{value}', va='center', ha='right', fontsize=30, 
            color=colors[i], fontproperties=prop)
        
        percentage_x = value_x + 5
        ax.text(percentage_x, y_pos, 
            f'%', va='center', ha='right', fontsize=15, 
            color=colors[i], fontproperties=prop)
        # Position range labels to the right of the percentage labels
        label_x = percentage_x + 5  # Add some spacing after percentage
        ax.text(label_x, y_pos, 
            labels[i], va='center', ha='left', fontsize=15, 
            color="#545454", fontproperties=prop)
    
    # Add grid lines
    ax.grid(True, axis='x', alpha=0.3, linestyle='-', linewidth=0.5)
    ax.set_axisbelow(True)
    
    # Set x-axis limits to accommodate labels on the right
    ax.set_xlim(0, 140)  # Extended to accommodate the range labels
    
    ax.set_ylim(-0.5, len(values) - 0.5)
    # Invert y-axis to match the original chart order
    ax.invert_yaxis()
    
    # Adjust layout
    plt.tight_layout()
    
    return fig, ax

# Example usage
if __name__ == "__main__":
    # Example data matching the image approximately
    bg_values = [3.2, 24, 69.9, 2.2, 0.7]
    
    # Create the chart
    fig, ax = create_bg_distribution_chart(bg_values)
    
    # Display the chart
    plt.show()
    
    # Optional: Save the chart
    # plt.savefig('bg_distribution.png', dpi=300, bbox_inches='tight')
