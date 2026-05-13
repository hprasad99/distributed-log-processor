package org.project.logprocessor.consumer.repository;

import java.time.LocalDateTime;
import java.util.List;
import org.project.logprocessor.consumer.model.LogEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface LogEventRepository extends JpaRepository<LogEvent, String> {
  List<LogEvent> findByOrganizationIdAndTimestampBetween(
      String organizationId, LocalDateTime start, LocalDateTime end);

  @Query("SELECT COUNT(l) FROM LogEvent l WHERE l.organizationId = :orgId AND l.level = :level")
  long countByOrganizationIdAndLevel(
      @Param("orgId") String organizationId, @Param("level") String level);

  List<LogEvent> findByLevelAndTimestampAfter(String level, LocalDateTime localDateTime);
}
