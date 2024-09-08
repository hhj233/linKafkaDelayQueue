package com.lin.manager.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author linzj
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DelayTopicConsumerLagLeadDto {
    /**
     * endOffset - consumerCommitOffset
     */
    private Long lag;

    /**
     * consumerCommitOffset - beginOffset
     */
    private Long lead;
}
