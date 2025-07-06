"""
Visualization processor for Reddit data.
"""

import logging
from pathlib import Path
import plotly.express as px
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class VisualizationProcessor(BaseProcessor):
    """Processor for creating visualizations."""
    
    def __init__(self, output_dir: Path = None):
        """
        Initialize the visualization processor.
        
        Args:
            output_dir: Directory for output files
        """
        super().__init__(output_dir)
        self.setup_directories('plots')
        
    def create_engagement_visualizations(self, df, batch_id):
        """
        Create visualizations for engagement metrics.
        
        Args:
            df: DataFrame with the batch data
            batch_id: ID of the current batch
            
        Returns:
            Dictionary with paths to visualization files
        """
        try:
            visualizations = {}
            
            # Create engagement distribution plot
            fig_dist = px.histogram(
                df,
                x='num_comments',
                title='Comment Distribution',
                labels={'num_comments': 'Number of Comments'},
                template='plotly_white',
                nbins=20
            )
            dist_path = self.output_dir / "plots" / f"engagement_distribution_{batch_id}.html"
            fig_dist.write_html(str(dist_path))
            visualizations['engagement_distribution'] = dist_path
            
            # Create score vs comments scatter plot
            fig_scatter = px.scatter(
                df,
                x='score',
                y='num_comments',
                title='Score vs Comments',
                labels={'score': 'Post Score', 'num_comments': 'Number of Comments'},
                template='plotly_white',
                trendline="ols"
            )
            scatter_path = self.output_dir / "plots" / f"score_vs_comments_{batch_id}.html"
            fig_scatter.write_html(str(scatter_path))
            visualizations['score_vs_comments'] = scatter_path
            
            # Create top engaged posts bar chart (top 10)
            top_posts = df.nlargest(10, 'num_comments').copy()
            top_posts['title_short'] = top_posts['title'].apply(
                lambda x: x[:50] + '...' if len(x) > 50 else x
            )
            
            fig_top = px.bar(
                top_posts,
                x='title_short',
                y='num_comments',
                title='Top 10 Engaged Posts',
                labels={'title_short': 'Post Title', 'num_comments': 'Number of Comments'},
                template='plotly_white'
            )
            fig_top.update_layout(xaxis_tickangle=45)
            top_path = self.output_dir / "plots" / f"top_engaged_posts_{batch_id}.html"
            fig_top.write_html(str(top_path))
            visualizations['top_engaged_posts'] = top_path
            
            # Create engagement by sentiment
            if 'sentiment' in df.columns:
                fig_sentiment = px.box(
                    df,
                    x='sentiment',
                    y='num_comments',
                    title='Engagement by Sentiment',
                    labels={'sentiment': 'Sentiment', 'num_comments': 'Number of Comments'},
                    template='plotly_white'
                )
                sentiment_path = self.output_dir / "plots" / f"engagement_by_sentiment_{batch_id}.html"
                fig_sentiment.write_html(str(sentiment_path))
                visualizations['engagement_by_sentiment'] = sentiment_path
            
            # Create engagement by topic
            if 'topic' in df.columns:
                topic_engagement = df.groupby('topic')['num_comments'].agg(['mean', 'count']).reset_index()
                topic_engagement = topic_engagement.sort_values('mean', ascending=False)
                
                fig_topic = px.bar(
                    topic_engagement,
                    x='topic',
                    y='mean',
                    title='Average Engagement by Topic',
                    labels={'topic': 'Topic ID', 'mean': 'Average Comments'},
                    template='plotly_white',
                    text='count'
                )
                fig_topic.update_traces(texttemplate='%{text} posts', textposition='outside')
                topic_path = self.output_dir / "plots" / f"engagement_by_topic_{batch_id}.html"
                fig_topic.write_html(str(topic_path))
                visualizations['engagement_by_topic'] = topic_path
            
            logger.info(f"Created engagement visualizations for batch {batch_id}")
            return visualizations
            
        except Exception as e:
            logger.error(f"Error creating engagement visualizations: {str(e)}")
            return {} 