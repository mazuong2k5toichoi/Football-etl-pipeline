import streamlit as st
from contextlib import contextmanager
import pandas as pd
from sqlalchemy import create_engine
from PIL import Image
import os
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
view= ['club_win_analysis','goal_formation','leaderboard','mart_player_transfer_analysis']
PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}
def init_connection(config):
    return psycopg2.connect(
        database=config['database'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

def extract_data():
    conn = init_connection(PSQL_CONFIG)
    return [pd.read_sql(f'SELECT * FROM analysis.{tab}', conn) for tab in view]

df= extract_data()
club= df[0]
formation= df[1]
leaderboard= df[2]
mart= df[3]



st.set_page_config(
    page_title="Football Analytics",
    page_icon="📔",
    layout="centered",
    initial_sidebar_state="expanded",
)

# Create sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox("Select a page", ["Home", "Leaderboard", "Club Analysis", "Formation Analysis", "Players Stats"])

# Home page
if page == "Home":
    st.title("Football Analytics Dashboard ⚽") 
    
    # Add a welcome section with dashboard overview
    st.write("""
    ### Welcome to Football Analytics Central! 
    
    This dashboard provides comprehensive insights into football competitions, clubs, formations, and players. 
    Use the sidebar navigation to explore different aspects of football analytics:
    
    - **Leaderboard**: View season standings for various competitions
    - **Club Analysis**: Analyze club performance metrics and squad details
    - **Formation Analysis**: Explore tactical trends across different competitions
    - **Players Stats**: Track player transfer histories and market value changes
    """)
    
    # Add a visual divider
    st.markdown("---")
    
    # Add section for competition selection with better explanation
    st.header("Competition Overview")
    st.write("Select a competition below to view key metrics across seasons, including total goals and average attendance.")
    
    # Get unique competitions
    competitions = sorted(formation['name'].unique())
    selected_comp = st.selectbox("Select Competition", competitions, key="home_comp")
    
    # Filter data for selected competition
    filtered_formation = formation[(formation['name'] == selected_comp) & (formation['season'] != 2012)]
    if not filtered_formation.empty:
        # Sort by season for chronological display
        filtered_formation = filtered_formation.sort_values('season') 
        
        # Add competition information summary
        total_seasons = len(filtered_formation)
        avg_goals_per_season = filtered_formation['total_goals'].mean()
        avg_attendance = filtered_formation['avg_attendance'].mean()
        
        # Display summary metrics
        st.subheader(f"{selected_comp} Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Seasons", f"{total_seasons}")
        with col2:
            st.metric("Avg. Goals per Season", f"{avg_goals_per_season:.1f}")
        with col3:
            st.metric("Overall Avg. Attendance", f"{avg_attendance:,.0f}")
            
        # Create visualization in tabs for better organization
        tab1, tab2 = st.tabs(["Goals & Attendance Chart", "Additional Stats"])
        
        with tab1:
            # Create a mixed chart with matplotlib
            fig, ax1 = plt.subplots(figsize=(12, 8))
            
            # Bar chart for total goals
            x = np.arange(len(filtered_formation))
            width = 0.4
            bars = ax1.bar(x, filtered_formation['total_goals'], width, color='#3498db', label='Total Goals')
            ax1.set_ylabel('Total Goals', color='#3498db', fontsize=12)
            ax1.tick_params(axis='y', labelcolor='#3498db')
            
            # Add value labels to bars
            for bar in bars:
                height = bar.get_height()
                ax1.annotate(f'{height:.0f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),  # 3 points vertical offset
                            textcoords="offset points",
                            ha='center', va='bottom',
                            fontsize=9)
            
            # Create second y-axis
            ax2 = ax1.twinx()
            # Line chart for average attendance
            line = ax2.plot(x, filtered_formation['avg_attendance'], color='#e74c3c', 
                           marker='o', linewidth=3, label='Avg. Attendance')
            ax2.set_ylabel('Average Attendance', color='#e74c3c', fontsize=12)
            ax2.tick_params(axis='y', labelcolor='#e74c3c')
            
            # Add value labels to the line points
            for i, v in enumerate(filtered_formation['avg_attendance']):
                ax2.annotate(f'{v:,.0f}',
                            xy=(i, v),
                            xytext=(0, 10),
                            textcoords="offset points",
                            ha='center',
                            fontsize=9)
            
            # Set x-axis labels
            ax1.set_xticks(x)
            ax1.set_xticklabels(filtered_formation['season'], rotation=45)
            ax1.set_xlabel('Season', fontsize=12)
            
            # Add title and grid
            plt.title(f"{selected_comp} - Goals and Attendance by Season", fontsize=14, fontweight='bold')
            ax1.grid(True, linestyle='--', alpha=0.7)
            
            # Add legend
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
            
            plt.tight_layout()
            st.pyplot(fig)
            
            # Add interpretation text
            st.markdown("""
            **Chart Interpretation:**
            - **Blue bars** show the total goals scored in each season
            - **Red line** shows the average attendance per match
            
            This visualization helps identify any correlation between goal-scoring trends and fan attendance.
            """)
            
        with tab2:
            # Show a data table with all key metrics
            st.subheader("Detailed Season Stats")
            
            # Prepare the data table with additional metrics
            display_df = filtered_formation[['season', 'total_goals', 'avg_attendance',
                                          'best_formation', 'best_formation_win_rate']].copy()
            
            # Convert win rate to percentage
            display_df['best_formation_win_rate'] = display_df['best_formation_win_rate'] / 100
            
            # Rename columns for display
            display_df = display_df.rename(columns={
                'season': 'Season',
                'total_goals': 'Total Goals',
                'avg_attendance': 'Avg. Attendance',
                'best_formation': 'Most Successful Formation',
                'best_formation_win_rate': 'Win Rate'
            })
            
            # Display the formatted table
            st.dataframe(
                display_df.style
                .set_properties(**{'text-align': 'center'})
                .set_table_styles([
                    {'selector': 'th', 'props': [('font-weight', 'bold'), ('text-align', 'center')]},
                ])
                .format({
                    'Total Goals': '{:.0f}',
                    'Avg. Attendance': '{:,.0f}',
                    'Win Rate': '{:.1%}'
                }),
                use_container_width=True
            )
            
            # Add a button to explore more in Formation Analysis
            if st.button("Explore Formations in Detail"):
                # This sets a session state to navigate to Formation Analysis page
                st.session_state.page = "Formation Analysis"
                st.experimental_rerun()
    else:
        st.info(f"No data available for {selected_comp}. Please select another competition.")
        
    # Add a footer with dashboard information
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: gray; font-size: 0.8em;">
        Football Analytics Dashboard | Created with Streamlit | Data updated periodically
    </div>
    """, unsafe_allow_html=True)

# Leaderboard page
elif page == "Leaderboard":
    st.title("Competition Leaderboards 🏆")
    
    # Get unique competitions and seasons
    competitions = sorted(leaderboard['competition_name'].unique())
    
    # Create selection widgets
    selected_competition = st.selectbox("Select Competition", competitions)
    
    # Filter leaderboard based on selection
    filtered_leaderboard = leaderboard[leaderboard['competition_name'] == selected_competition]
    
    # Get available seasons for the selected competition
    available_seasons = sorted(filtered_leaderboard['season'].unique())
    selected_season = st.selectbox("Select Season", available_seasons)
    
    # Filter further by season
    final_table = filtered_leaderboard[filtered_leaderboard['season'] == selected_season]
    
    # Sort by position
    final_table = final_table.sort_values(by='position')
    
    # Display the leaderboard
    st.subheader(f"{selected_competition} - {selected_season} Season")
    
    # Format the table for display
    display_columns = ['position', 'club_name', 'games_played', 'wins', 'draws', 
                      'losses', 'goals_for', 'goals_against', 'goal_difference', 'points']
    
    if not final_table.empty:
        # Style the leaderboard table
        st.dataframe(
            final_table[display_columns].style
            .set_properties(**{'text-align': 'center'})
            .set_table_styles([
                {'selector': 'th', 'props': [('font-weight', 'bold'), ('text-align', 'center')]},
            ])
            .format({
                'position': '{:.0f}',
                'games_played': '{:.0f}',
                'wins': '{:.0f}',
                'draws': '{:.0f}',
                'losses': '{:.0f}',
                'goals_for': '{:.0f}',
                'goals_against': '{:.0f}',
                'goal_difference': '{:.0f}',
                'points': '{:.0f}'
            }),
            use_container_width=True
        )
        
        # Add visualization - top 5 teams by points
        st.subheader("Top Teams Comparison")
        top_teams = final_table.sort_values(by='points', ascending=False).head(5)

        # Create a more robust visualization using matplotlib instead of st.bar_chart
        fig, ax = plt.subplots(figsize=(12, 6))

        # Get team names and metrics
        team_names = top_teams['club_name']
        points = top_teams['points']
        goals_for = top_teams['goals_for']
        goals_against = top_teams['goals_against']

        # Set bar positions
        x = np.arange(len(team_names))
        width = 0.25

        # Create bars for each metric
        ax.bar(x - width, points, width, label='Points', color='#3498db')
        ax.bar(x, goals_for, width, label='Goals For', color='#2ecc71')
        ax.bar(x + width, goals_against, width, label='Goals Against', color='#e74c3c')

        # Add labels and styling
        ax.set_ylabel('Count')
        ax.set_title(f'Top 5 Teams in {selected_competition} - {selected_season}')
        ax.set_xticks(x)
        ax.set_xticklabels(team_names, rotation=45, ha='right')
        ax.legend()

        plt.tight_layout()
        st.pyplot(fig)

        # Add a data table below the chart for detailed information
        st.write("Detailed Statistics:")
        st.dataframe(
            top_teams[['club_name', 'points', 'wins', 'draws', 'losses', 'goals_for', 'goals_against', 'goal_difference']].reset_index(drop=True),
            use_container_width=True
        )
    else:
        st.write("No data available for the selected competition and season.")

# Placeholder for other pages

elif page == "Club Analysis":
    st.title("Club Analysis 🏟️")
    
    # Introduction
    st.write("""
    This section allows you to explore club performance data across different seasons.
    Analyze win rates, manager influence, and squad details for your selected club.
    """)
    
    # Get unique clubs
    clubs = sorted(club['club_name'].unique())
    
    # Club selection
    selected_club = st.selectbox("Select Club", clubs, help="Choose a club to analyze")
    
    # Filter club data
    club_data = club[club['club_name'] == selected_club].sort_values(by='season', ascending=False)
    
    if not club_data.empty:
        # Create tab layout
        tab1, tab2 = st.tabs(["Performance Analysis", "Current Squad"])
        
        with tab1:
            st.subheader(f"{selected_club} Performance Analysis")
            
            # Overall club stats
            latest_season = club_data.iloc[0]['season']
            latest_data = club_data.iloc[0]
            
            # Create metrics row
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Latest Season", f"{latest_season}")
            with col2:
                st.metric("Current Win Rate", f"{latest_data['win_rate']/100:.1%}")
            with col3:
                st.metric("Current Manager", latest_data['manager_name'])
            # with col4:
            #     total_wins = club_data['wins'].sum()
            #     st.metric("Total Wins", f"{int(total_wins)}")
            
            # Performance Over Time chart
            st.subheader("Performance Evolution")
            
            # Prepare data for visualization
            performance_data = club_data.sort_values('season')
            
            # Create performance visualization
            fig, ax1 = plt.subplots(figsize=(12, 6))
            
            # Plot win rate as a line
            ax1.set_xlabel('Season')
            ax1.set_ylabel('Win Rate', color='#1f77b4')
            ax1.plot(performance_data['season'], performance_data['win_rate'], 
                    marker='o', linewidth=3, color='#1f77b4', label='Win Rate')
            ax1.tick_params(axis='y', labelcolor='#1f77b4')
            ax1.set_ylim([0, max(performance_data['win_rate']) * 1.2])
            
            # Add win rate percentage labels
            for i, row in performance_data.iterrows():
                ax1.annotate(f"{row['win_rate']/100:.1%}", 
                           (row['season'], row['win_rate']),
                           textcoords="offset points",
                           xytext=(0,10), 
                           ha='center',
                           fontsize=9)
            
            # Create second y-axis for total games
            ax2 = ax1.twinx()
            ax2.set_ylabel('Total Games', color='#ff7f0e')
            ax2.bar(performance_data['season'], performance_data['total_games'], 
                   alpha=0.3, color='#ff7f0e', label='Total Games')
            ax2.tick_params(axis='y', labelcolor='#ff7f0e')
            
            # Add title and style
            plt.title(f'{selected_club} Performance by Season')
            plt.grid(True, linestyle='--', alpha=0.7)
            
            # Add a combined legend
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
            
            plt.tight_layout()
            st.pyplot(fig)
            
            # Manager Impact Analysis
            st.subheader("Manager Impact Analysis")
            
            # Group by manager and calculate average win rate
            manager_analysis = club_data.groupby('manager_name').agg({
                'win_rate': 'mean',
                'wins': 'sum',
                'total_games': 'sum',
                'season': 'count'
            }).reset_index()
            
            manager_analysis = manager_analysis.rename(columns={
                'win_rate': 'avg_win_rate',
                'season': 'seasons_managed'
            })
            
            # Sort by average win rate
            manager_analysis = manager_analysis.sort_values('avg_win_rate', ascending=False)
            
            # Display manager analysis
            fig_mgr, ax_mgr = plt.subplots(figsize=(12, 5))
            
            # Create bar chart
            bars = ax_mgr.bar(manager_analysis['manager_name'], manager_analysis['avg_win_rate']/100, 
                             color='#2ecc71')
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                ax_mgr.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                          f'{height:.1%}',
                          ha='center', va='bottom', fontsize=10)
            
            ax_mgr.set_ylim(0, max(manager_analysis['avg_win_rate']/100) * 1.2)
            ax_mgr.set_ylabel('Average Win Rate')
            ax_mgr.set_title(f'Manager Performance at {selected_club}')
            plt.xticks(rotation=45, ha='right')
            
            plt.tight_layout()
            st.pyplot(fig_mgr)
            
            # Display manager data table
            manager_analysis['avg_win_rate'] = manager_analysis['avg_win_rate']/100
            
            st.dataframe(
                manager_analysis.style
                .format({
                    'avg_win_rate': '{:.1%}',
                    'wins': '{:.0f}',
                    'total_games': '{:.0f}',
                    'seasons_managed': '{:.0f}'
                }),
                use_container_width=True
            )
            
        # Display current squad information
        with tab2:
            st.subheader(f"{selected_club} Current Squad")
            
            # Filter players currently at the club
            current_players = mart[mart['current_club_name'] == selected_club]
            
            # Make sure we have unique players (highest market value for duplicates)
            current_players = current_players.sort_values('current_market_value_in_eur', ascending=False)
            current_players = current_players.drop_duplicates(subset=['player_name'])
            
            if not current_players.empty:
                # Group by position
                player_positions = current_players.groupby('position').size().reset_index(name='count')
                
                # Create a positions pie chart
                fig_pos, ax_pos = plt.subplots(figsize=(8, 8))
                colors = plt.cm.tab10(np.linspace(0, 1, len(player_positions)))
                
                ax_pos.pie(player_positions['count'], labels=player_positions['position'], 
                         autopct='%1.1f%%', startangle=90, colors=colors,
                         wedgeprops={'edgecolor': 'white', 'linewidth': 1})
                
                ax_pos.axis('equal')
                plt.title('Squad Composition by Position')
                st.pyplot(fig_pos)
                
                # Create expandable section for squad market values
                with st.expander("Squad Market Values", expanded=True):
                    # Sort players by market value
                    top_players = current_players.sort_values('current_market_value_in_eur', ascending=False).head(10)
                    
                    # Create bar chart
                    fig_val, ax_val = plt.subplots(figsize=(12, 6))
                    
                    bars = ax_val.barh(top_players['player_name'], top_players['current_market_value_in_eur'],
                                     color=plt.cm.viridis(np.linspace(0, 0.8, len(top_players))))
                    
                    # Add value labels
                    for bar in bars:
                        width = bar.get_width()
                        ax_val.text(width * 1.01, bar.get_y() + bar.get_height()/2,
                                  f'€{width:,.0f}',
                                  ha='left', va='center', fontsize=9)
                    
                    ax_val.set_xlabel('Current Market Value (€)')
                    ax_val.set_title(f'Top 10 Most Valuable Players at {selected_club}')
                    ax_val.invert_yaxis()  # Put highest values at top
                    
                    # Format x-axis as currency
                    import matplotlib.ticker as mtick
                    fmt = '€{x:,.0f}'
                    ax_val.xaxis.set_major_formatter(mtick.StrMethodFormatter(fmt))
                    
                    plt.tight_layout()
                    st.pyplot(fig_val)
                
                # Display player table with links to Transfer Analysis page
                st.subheader("Squad List")
                st.write("Click on a player name to view their detailed transfer history.")
                
                # Prepare player display data
                player_display = current_players[['player_name', 'position', 'date_of_birth', 'current_market_value_in_eur']].copy()
                
                # Create clickable links
                def make_clickable(player):
                    return f"<a href='#' onclick='script:stSessionState.page=\"Players Stats\"; setTimeout(function() {{document.querySelector(\"select[data-testid=\'stSelectbox\']\").value=\"{player}\";document.querySelector(\"select[data-testid=\'stSelectbox\']\").dispatchEvent(new Event(\"change\"));}}, 100); '>{player}</a>"
                
                player_display['player_name'] = player_display['player_name'].apply(make_clickable)
                
                # Display the table with clickable links
                st.write(
                    player_display.rename(columns={
                        'player_name': 'Player Name',
                        'position': 'Position',
                        'date_of_birth': 'Date of Birth',
                        'current_market_value_in_eur': 'Market Value (€)'
                    }).style.format({
                        'Market Value (€)': '€{:,.0f}'
                    }).to_html(escape=False),
                    unsafe_allow_html=True
                )
                
                # Add custom JavaScript to make the links work with Streamlit
                st.markdown("""
                <script>
                window.stSessionState = {};
                
                Object.defineProperty(window.stSessionState, 'page', {
                    set: function(value) {
                        const sidebar = document.querySelector('.stSelectbox');
                        sidebar.value = value;
                        sidebar.dispatchEvent(new Event('change'));
                    }
                });
                </script>
                """, unsafe_allow_html=True)
                
            else:
                st.info(f"No current player data available for {selected_club}.")
    else:
        st.info(f"No performance data available for {selected_club}.")

elif page == "Formation Analysis":
    st.title("Formation Analysis 📋")
    
    # Introduction
    st.write("""
    Football tactics and formations have evolved significantly over the years. 
    This analysis explores how popular formations have changed across different competitions and what these changes tell us about tactical trends in modern football.
    """)
    
    # Get unique competitions for filtering
    competitions = sorted(formation['name'].unique())
    selected_comp = st.selectbox("Select Competition", competitions, key="formation_comp")
    
    # Filter formation data
    filtered_data = formation[(formation['name'] == selected_comp) & (formation['season'] != 2012)]
    
    if not filtered_data.empty:
        # Group similar formations
        def group_formations(formation_name):
            if formation_name.startswith('4-3'):
                return '4-3-3 Variations'
            elif formation_name.startswith('4-2-3'):
                return '4-2-3-1 Variations'
            elif formation_name.startswith('3-4') or formation_name.startswith('3-5'):
                return '3 at the back'
            else:
                return formation_name
        
        # Add grouped formation column
        filtered_data['grouped_formation'] = filtered_data['best_formation'].apply(group_formations)
        
        # Display best formations by season
        st.header(f"Best Formations in {selected_comp}")
        
        # Sort chronologically
        filtered_data = filtered_data.sort_values('season')
        
        # Create a visual representation of formation trends
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # Map formation groups to colors for consistency (using a better color scheme)
        all_formation_groups = filtered_data['grouped_formation'].unique()
        colors = plt.cm.viridis(np.linspace(0, 0.9, len(all_formation_groups)))
        formation_colors = dict(zip(all_formation_groups, colors))
        
        # Create bubbles where size is win rate and color is formation group
        for i, row in filtered_data.iterrows():
            ax.scatter(
                row['season'], 
                0,  # All on same y-axis
                s=row['best_formation_win_rate'] * 1000,  # Scale win rate for visibility
                color=formation_colors[row['grouped_formation']],
                alpha=0.8,
                edgecolors='black',
                linewidth=1
            )
            # Display the specific formation inside the bubble
            ax.annotate(row['best_formation'], 
                       (row['season'], 0), 
                       ha='center', 
                       va='center',
                       fontweight='bold',
                       fontsize=9,
                       color='white')
            
        # Add formation legend
        formation_patches = [plt.Line2D([0], [0], marker='o', color='w', 
                            markerfacecolor=color, markersize=10, label=formation)
                            for formation, color in formation_colors.items()]
        
        # Improved legend placement and styling
        ax.legend(handles=formation_patches, title="Formation Groups", 
                 loc='upper center', bbox_to_anchor=(0.5, -0.05),
                 fancybox=True, shadow=True, ncol=3, fontsize=10)
        
        # Customize plot
        ax.set_yticks([])  # Hide y axis ticks
        ax.set_xlabel('Season', fontsize=14, fontweight='bold')
        ax.set_title(f'Formation Evolution in {selected_comp}', fontsize=18, fontweight='bold')
        ax.grid(True, axis='x', linestyle='--', alpha=0.7)
        
        # Add win rate explanation
        win_rate_explanation = "Note: Bubble size represents win rate percentage"
        ax.text(filtered_data['season'].min(), -0.2, win_rate_explanation, 
               fontsize=12, ha='left', transform=ax.transData)
        
        # Style x-axis
        plt.xticks(fontsize=11)
        
        plt.tight_layout(rect=[0, 0.1, 1, 0.95])  # Adjust layout for legend
        st.pyplot(fig)
        
        # Add a formation trend explanation
        st.subheader("Formation Trends")
        
        # Count occurrences of each formation group
        formation_counts = filtered_data['grouped_formation'].value_counts()
        
        # Create a horizontal bar chart for formation distribution
        fig_dist, ax_dist = plt.subplots(figsize=(10, 4))
        bars = ax_dist.barh(formation_counts.index, formation_counts.values, 
                           color=[formation_colors[f] for f in formation_counts.index])
        ax_dist.set_xlabel('Number of Seasons', fontsize=12)
        ax_dist.set_title('Formation Group Distribution', fontsize=14)
        
        # Add value labels to the bars
        for bar in bars:
            width = bar.get_width()
            ax_dist.text(width + 0.1, bar.get_y() + bar.get_height()/2, 
                        f'{width:.0f}', ha='left', va='center', fontsize=10)
            
        plt.tight_layout()
        st.pyplot(fig_dist)
        
        # Display table with detailed information
        st.subheader("Formation Details by Season")
        
        # Prepare the table with both original and grouped formations
        formation_table = filtered_data[['season', 'best_formation', 'grouped_formation', 'best_formation_win_rate', 'total_goals']]
        formation_table['best_formation_win_rate'] = formation_table['best_formation_win_rate'] / 100

        st.dataframe(
            formation_table.style
            .set_properties(**{'text-align': 'center'})
            .set_table_styles([
                {'selector': 'th', 'props': [('font-weight', 'bold'), ('text-align', 'center')]},
            ])
            .format({
                'best_formation_win_rate': '{:.1%}',
                'total_goals': '{:,.0f}'
            }),
            use_container_width=True
        )
        
        # Analysis section
        st.header("Tactical Analysis")
        
        st.subheader("Common Formations Over the Years")
        st.write("""
        - **4-3-3 Variations**: This formation family is frequently used across multiple leagues and competitions. It's seen in the Premier League, 
        La Liga, Bundesliga, and UEFA Champions League consistently. The widespread use indicates its popularity due to 
        its balanced nature, providing both strong defense and attacking possibilities.
        
        - **4-2-3-1 Variations**: This formation group is also popular and is often seen in competitions like the UEFA Champions League, 
        emphasizing a solid midfield and flexible attack. It provides good defensive coverage while maintaining attacking options.
        
        - **3 at the back Systems**: These formations (3-4-3, 3-5-2, etc.) have seen increased usage across different leagues, 
        highlighting a trend towards using three central defenders while maintaining width with wingbacks.
        """)
        
        # Rest of your analysis content...
    else:
        st.write("No formation data available for the selected competition.")




elif page == "Players Stats":
    st.title("Players Stats 💰")
    
    # Introduction
    st.write("""
    This section allows you to explore player transfer histories and market value changes over time.
    Search for a player to view their transfer details and career progression.
    """)
    
    # Player search functionality
    st.subheader("Player Search")
    
    # Create search box with autocomplete
    all_players = sorted(mart['player_name'].unique())
    player_name = st.selectbox(
        "Search for a player:",
        options=all_players,
        help="Type to search for a player"
    )
    
    # Filter data for selected player
    player_data = mart[mart['player_name'] == player_name]
    
    if not player_data.empty:
        # Get player's basic info (using the most recent data)
        recent_data = player_data.sort_values('transfer_season', ascending=False).iloc[0]
        
        # Create two column layout
        col1, col2 = st.columns([1, 2])
        
        with col1:
            # Display player image
            if pd.notna(recent_data['image_url']):
                try:
                    st.image(recent_data['image_url'], caption=player_name, width=200)
                except:
                    st.error("Could not load player image")
                    st.write("⚽ No image available")
            else:
                st.write("⚽ No image available")
                
            # Display player details
            st.subheader("Player Information")
            player_info = {
                "Full Name": player_name,
                "Date of Birth": recent_data['date_of_birth'],
                "Position": recent_data['position'],
                "Hometown": recent_data['hometown'],
                "Preferred Foot": recent_data['foot'],
                "Height": f"{recent_data['height_in_cm']} cm" if pd.notna(recent_data['height_in_cm']) else "Unknown",
                "Current Club": recent_data['current_club_name'],
                "Current Market Value": f"€{recent_data['current_market_value_in_eur']:,.0f}" if pd.notna(recent_data['current_market_value_in_eur']) else "Unknown",
                "Highest Market Value": f"€{recent_data['highest_market_value_in_eur']:,.0f}" if pd.notna(recent_data['highest_market_value_in_eur']) else "Unknown"
            }
            
            for key, value in player_info.items():
                st.write(f"**{key}:** {value}")
                
        with col2:
            st.subheader("Transfer History")
            
            # Create a dataframe for transfer history
            transfers = player_data.sort_values('transfer_season')
            
            # Display transfer table
            transfer_display = transfers[['transfer_season', 'from_club_name', 'to_club_name', 'transfer_market_value_in_eur']]
            transfer_display = transfer_display.rename(columns={
                'transfer_season': 'Season',
                'from_club_name': 'From Club',
                'to_club_name': 'To Club',
                'transfer_market_value_in_eur': 'Value (€)'
            })
            
            st.dataframe(
                transfer_display.style.format({
                    'Value (€)': '€{:,.0f}'
                }),
                use_container_width=True
            )
            
            # Create market value progression chart
            st.subheader("Market Value Progression")
            
            if len(transfers) > 0 and transfers['transfer_market_value_in_eur'].notna().any():
                fig, ax = plt.subplots(figsize=(10, 6))
                
                # Plot transfer values
                ax.plot(transfers['transfer_season'], 
                       transfers['transfer_market_value_in_eur'], 
                       marker='o', 
                       linewidth=2,
                       markersize=8,
                       color='#1f77b4')
                
                # Add labels to points
                for i, row in transfers.iterrows():
                    if pd.notna(row['transfer_market_value_in_eur']):
                        ax.annotate(
                            f"€{row['transfer_market_value_in_eur']:,.0f}",
                            (row['transfer_season'], row['transfer_market_value_in_eur']),
                            textcoords="offset points",
                            xytext=(0,10),
                            ha='center',
                            fontsize=9
                        )
                        # Add club names
                        ax.annotate(
                            f"{row['to_club_name']}",
                            (row['transfer_season'], row['transfer_market_value_in_eur']),
                            textcoords="offset points",
                            xytext=(0,-15),
                            ha='center',
                            fontsize=8,
                            rotation=45
                        )
                
                # Add styling
                ax.set_xlabel('Transfer Season')
                ax.set_ylabel('Transfer Value (€)')
                ax.set_title(f'Market Value Evolution: {player_name}')
                ax.grid(True, linestyle='--', alpha=0.7)
                
                # Format y-axis as currency
                import matplotlib.ticker as mtick
                fmt = '€{x:,.0f}'
                ax.yaxis.set_major_formatter(mtick.StrMethodFormatter(fmt))
                
                plt.tight_layout()
                st.pyplot(fig)
            else:
                st.info("No market value data available for this player's transfers")
            
            # Add transfer summary statistics
            st.subheader("Transfer Summary")
            
            summary_cols = st.columns(3)
            with summary_cols[0]:
                st.metric("Total Transfers", int(recent_data['total_transfers']))
            with summary_cols[1]:
                value_change = recent_data['value_change']
                if pd.notna(value_change):
                    delta_color = "normal" if value_change >= 0 else "inverse"
                    st.metric("Value Change", f"€{value_change:,.0f}", delta=f"{recent_data['value_change_percentage']:.1f}%", delta_color=delta_color)
                else:
                    st.metric("Value Change", "N/A")
            with summary_cols[2]:
                st.metric("Career Span", f"{recent_data['first_transfer_season']} - {recent_data['last_transfer_season']}")
            
    else:
        st.info("Please select a player to view their transfer history.")
        
        # Display some example insights about transfers in general
        st.subheader("Transfer Market Insights")
        st.write("""
        While you decide which player to explore, here are some interesting facts about the football transfer market:
        
        - The global football transfer market has seen significant growth over the past decades
        - Transfer fees have been increasingly inflated, especially for young promising talents
        - Clubs with strong academies often generate substantial revenue through player sales
        - Market values fluctuate based on performance, injuries, contract duration, and age
        """)