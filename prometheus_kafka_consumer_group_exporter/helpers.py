def ensure_dict_key(curr_dict, key, new_value):
    if key in curr_dict:
        return curr_dict

    new_dict = curr_dict.copy()
    new_dict[key] = new_value
    return new_dict


def clear_commits(dict):
    for group, group_value in dict.items():
        for topic, topic_value in group_value.items():
            for partition, partition_value in topic_value.items():
                dict[group][topic][partition] = 0