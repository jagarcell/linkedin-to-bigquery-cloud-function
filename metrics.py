# The dimensions to use as pivots in the LinkedIn Analytics API requests
PIVOTS = ['CAMPAIGN', 'CAMPAIGN_GROUP']

###########################################################################################################################################################
# BigQuery table schemas                                                                                                                                  #
# These BigQuery tables must be created under the dataset created at https://console.cloud.google.com/bigquery?project=media-analytics-473214&authuser=1  #
# They must have a basic schema with the following fields:                                                                                                #  
#  - date (DATE)                                                                                                                                          #
#  - account_name (STRING)                                                                                                                                #
#  - account_id (STRING)                                                                                                                                  #
#  - campaign_group_name (STRING)                                                                                                                         #
#  - campaign_group_id (STRING)                                                                                                                           #
#  - campaign_name (STRING)                                                                                                                               #
#  - campaign_id (STRING)                                                                                                                                 #
#  - campaign_type (STRING)                                                                                                                               #
#  - campaign_status (STRING)                                                                                                                             #
###########################################################################################################################################################

# You will have to add the desired metric columns to the previous schema for each data table as per the metrics listed below

# Here are added the table names and the metrics to be pulled for each one, this metrics must match both LinkedIn API and the table schema created in BigQuery
BIGQUERY_TABLES = [
    {
        'ad_analytics': {
            'metrics': [
                'costInUsd',
                'impressions',
                'cardImpressions',
                'clicks',
                'cardClicks',
                'oneClickLeads',
                'oneClickLeadFormOpens',
                'validWorkEmailLeads',
                'sends',
                'opens',
                'shares',
                'comments',
                'reactions',
                'totalEngagements'
            ]
        }
    },
    {
        'ad_click_metrics' :{
            'metrics': [
                'clicks',
                'actionClicks',
                'adUnitClicks',
                'cardClicks',
                'companyPageClicks',
                'headlineClicks',
                'landingPageClicks',
                'subscriptionClicks',
                'textUrlClicks',
                'viralClicks',
                'viralCardClicks',
                'viralCompanyPageClicks',
                'viralLandingPageClicks',
                'viralSubscriptionClicks'
            ]
        },
    },
    {
        'ad_conversion_metrics':{
            'metrics': [
                'externalWebsiteConversions',
                'externalWebsitePostClickConversions',
                'externalWebsitePostViewConversions',
                'qualifiedLeads',
                'validWorkEmailLeads',
                'costPerQualifiedLead',
                'conversionValueInLocalCurrency',
                'viralExternalWebsiteConversions',
                'viralExternalWebsitePostClickConversions',
                'viralExternalWebsitePostViewConversions'
            ]
        },
    },
    {
        'ad_cost_metrics':{
            'metrics': [
                'costInLocalCurrency',
                'costInUsd',
                'costPerQualifiedLead',
                'conversionValueInLocalCurrency',
                'averageDwellTime'
            ]
        },
    },
    {
        'ad_delivery_metrics':{
            'metrics': [
                'impressions',
                'cardImpressions',
                'headlineImpressions',
                'approximateMemberReach',
                'audiencePenetration',
                'viralImpressions',
                'viralCardImpressions'
            ]
        },
    },
    {
        'ad_engagement_metrics':{
            'metrics': [
                'likes',
                'reactions',
                'commentLikes',
                'comments',
                'shares',
                'otherEngagements',
                'totalEngagements',
                'viralLikes',
                'viralReactions',
                'viralCommentLikes',
                'viralComments',
                'viralShares',
                'viralOtherEngagements',
                'viralTotalEngagements'
            ]
        },
    },
    {
        'ad_job_metrics':{
            'metrics': [
                'jobApplications',
                'jobApplyClicks',
                'postClickJobApplications',
                'postClickJobApplyClicks',
                'postClickRegistrations',
                'postViewJobApplications',
                'postViewJobApplyClicks',
                'postViewRegistrations',
                'viralJobApplications',
                'viralJobApplyClicks',
                'viralPostClickJobApplications',
                'viralPostClickJobApplyClicks',
                'viralPostClickRegistrations',
                'viralPostViewJobApplications',
                'viralPostViewJobApplyClicks',
                'viralPostViewRegistrations',
                'registrations',
                'talentLeads'
            ]
        },
    },
    {
        'ad_lead_form_metrics':{
            'metrics': [
                'oneClickLeads',
                'oneClickLeadFormOpens',
                'leadGenerationMailInterestedClicks',
                'leadGenerationMailContactInfoShares',
                'follows',
                'sends',
                'opens',
                'viralOneClickLeads',
                'viralOneClickLeadFormOpens',
                'viralFollows'
            ]
        },
    },
    {
        'ad_other_viral_metrics':{
            'metrics': [
                'viralDocumentCompletions',
                'viralDocumentFirstQuartileCompletions',
                'viralDocumentMidpointCompletions',
                'viralDocumentThirdQuartileCompletions',
                'viralDownloadClicks',
                'viralFullScreenPlays',
                'viralRegistrations'
            ]
        },
    },
    {
        'ad_video_doc_metrics':{
            'metrics': [
                'videoStarts',
                'videoViews',
                'videoCompletions',
                'videoFirstQuartileCompletions',
                'videoMidpointCompletions',
                'videoThirdQuartileCompletions',
                'fullScreenPlays',
                'documentCompletions',
                'documentFirstQuartileCompletions',
                'documentMidpointCompletions',
                'documentThirdQuartileCompletions',
                'downloadClicks',
                'viralVideoStarts',
                'viralVideoViews',
                'viralVideoCompletions',
                'viralVideoFirstQuartileCompletions',
                'viralVideoMidpointCompletions',
                'viralVideoThirdQuartileCompletions'
            ]
        }
    }
]
