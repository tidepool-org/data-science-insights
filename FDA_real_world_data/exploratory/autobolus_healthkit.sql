select
    origin,
    payload,
    get_json_object(origin, '$.payload.sourceRevision.source.name') as source_name,
    get_json_object(payload, '$[\"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued\"]') as automatically_issued,
    case
        when get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
         and get_json_object(payload, '$[\"com.loopkit.InsulinKit.MetadataKeyAutomaticallyIssued\"]') = '1'
        then 'AB'
        when get_json_object(origin, '$.payload.sourceRevision.source.name') = 'Loop'
        then 'TB'
    end as dosing_strategy
from bddp_sample_100
where payload like '%MetadataKeyAutomaticallyIssued%'