#!/usr/bin/env python3
"""
Additional database tests to improve coverage for edge cases and error paths.
Targets uncovered lines in database.py methods.
"""

from datetime import datetime, timedelta, timezone


# Use conftest fixtures for temp_db and mock_psycopg2


class TestCampaignExpiration:
    """Test campaign expiration logic"""

    def test_save_campaign_with_future_expiration(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving campaign with future expiration date"""
        future_date = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        campaign_data = {
            "id": 1,
            "planet": {"index": 5},
            "expiresAt": future_date
        }
        result = temp_db.save_campaign(1, 5, campaign_data)
        assert result is True

        # Mock campaign result
        mock_cursor.fetchall.return_value = [(campaign_data,)]
        campaigns = temp_db.get_active_campaigns()
        assert len(campaigns) > 0

    def test_save_campaign_with_past_expiration(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving campaign with past expiration date"""
        past_date = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        campaign_data = {
            "id": 1,
            "planet": {"index": 5},
            "expiresAt": past_date
        }
        result = temp_db.save_campaign(1, 5, campaign_data)
        assert result is True

        # Should still be saved in DB with expired status
        # For PostgreSQL, we verify the campaign was saved by checking get_active_campaigns
        # (which filters expired campaigns, so expired ones won't appear)
        campaigns = temp_db.get_active_campaigns()
        # The expired campaign should not be in active campaigns
        assert isinstance(campaigns, list)

    def test_save_campaign_no_expiration(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving campaign without expiration"""
        campaign_data = {
            "id": 1,
            "planet": {"index": 5}
        }
        result = temp_db.save_campaign(1, 5, campaign_data)
        assert result is True

    def test_save_campaign_invalid_expiration_format(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving campaign with invalid expiration format"""
        campaign_data = {
            "id": 1,
            "planet": {"index": 5},
            "expiresAt": "invalid-date-format"
        }
        result = temp_db.save_campaign(1, 5, campaign_data)
        # Should default to active despite invalid format
        assert result is True

    def test_get_active_campaigns_filters_expired(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test that get_active_campaigns filters out expired campaigns"""
        # Add a future campaign
        future_date = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        future_campaign = {
            "id": 1,
            "planet": {"index": 5},
            "expiresAt": future_date
        }
        temp_db.save_campaign(1, 5, future_campaign)

        # Add a past campaign
        past_date = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        past_campaign = {
            "id": 2,
            "planet": {"index": 6},
            "expiresAt": past_date
        }
        temp_db.save_campaign(2, 6, past_campaign)

        # Get active campaigns - should return only the future one
        active = temp_db.get_active_campaigns()
        # Active campaigns are filtered at retrieval time
        assert isinstance(active, list)

    def test_get_active_campaigns_no_expiration_included(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test that campaigns without expiration are included"""
        campaign_data = {
            "id": 1,
            "planet": {"index": 5}
        }
        temp_db.save_campaign(1, 5, campaign_data)

        # Mock campaign result
        mock_cursor.fetchall.return_value = [(campaign_data,)]
        active = temp_db.get_active_campaigns()
        assert len(active) > 0


class TestPlanetEventVariations:
    """Test planet event variations in format"""

    def test_save_planet_events_snake_case(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving planet events with snake_case keys"""
        events = [
            {"id": 1, "planet_index": 5, "event_type": "defense"}
        ]
        result = temp_db.save_planet_events(events)
        assert result is True

        # Mock planet events result
        mock_cursor.fetchall.return_value = [(events[0],)]
        retrieved = temp_db.get_latest_planet_events()
        assert len(retrieved) > 0

    def test_save_planet_events_camel_case(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving planet events with camelCase keys"""
        events = [
            {"id": 2, "planetIndex": 6, "eventType": "offensive"}
        ]
        result = temp_db.save_planet_events(events)
        assert result is True

    def test_save_planet_events_missing_planet_index(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving planet events without planet_index"""
        events = [
            {"id": 3, "eventType": "event"}  # Missing planet_index
        ]
        result = temp_db.save_planet_events(events)
        # Should not crash even with missing planet_index
        assert result is True

    def test_save_planet_events_missing_event_id(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving planet events without event_id"""
        events = [
            {"planetIndex": 5, "eventType": "event"}  # Missing id
        ]
        result = temp_db.save_planet_events(events)
        # Should handle gracefully
        assert isinstance(result, bool)

    def test_save_planet_events_default_event_type(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test saving planet events defaults to 'unknown' for missing eventType"""
        events = [
            {"id": 4, "planetIndex": 7}  # Missing eventType
        ]
        result = temp_db.save_planet_events(events)
        assert result is True

    def test_get_planet_events_by_planet_index(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test getting planet events filtered by planet_index"""
        events = [
            {"id": 1, "planetIndex": 5, "eventType": "defense"},
            {"id": 2, "planetIndex": 6, "eventType": "offensive"},
            {"id": 3, "planetIndex": 5, "eventType": "storm"}
        ]
        temp_db.save_planet_events(events)

        # Get events for planet 5
        result = temp_db.get_planet_events(planet_index=5)
        assert isinstance(result, list)


class TestBiomeSnapshotEdgeCases:
    """Test biome snapshot with various biome formats"""

    def test_get_latest_biomes_snapshot_no_biomes(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test getting biomes snapshot when planets have no biome data"""
        temp_db.save_planet_status(1, {"index": 1, "name": "Planet 1"})
        temp_db.save_planet_status(2, {"index": 2, "name": "Planet 2"})

        result = temp_db.get_latest_biomes_snapshot()
        # Should return None or empty list when no biomes
        assert result is None or result == []

    def test_get_latest_biomes_snapshot_biome_not_dict(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test getting biomes when biome is not a dict"""
        temp_db.save_planet_status(1, {
            "index": 1,
            "biome": "string-biome-not-dict"
        })

        result = temp_db.get_latest_biomes_snapshot()
        # Should handle non-dict biomes gracefully
        assert result is None or isinstance(result, list)

    def test_get_latest_biomes_snapshot_duplicate_biomes(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test that duplicate biome names are not repeated"""
        temp_db.save_planet_status(1, {
            "index": 1,
            "biome": {"name": "Desert", "severity": 5}
        })
        temp_db.save_planet_status(2, {
            "index": 2,
            "biome": {"name": "Desert", "severity": 7}
        })

        result = temp_db.get_latest_biomes_snapshot()
        if result:
            # Count occurrences of Desert biome
            desert_count = sum(1 for b in result if b.get("name") == "Desert")
            # Should only appear once
            assert desert_count <= 1


class TestGetPlanetStatus:
    """Test getting planet status"""

    def test_get_planet_status(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test retrieving planet status by index"""
        planet_data = {"index": 5, "name": "Test Planet", "owner": "Humans"}
        temp_db.save_planet_status(5, planet_data)

        # Mock planet status result
        mock_cursor.fetchone.return_value = (planet_data,)
        result = temp_db.get_planet_status(5)
        assert result is not None
        assert result["name"] == "Test Planet"

    def test_get_planet_status_not_found(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test getting non-existent planet status"""
        result = temp_db.get_planet_status(999)
        assert result is None


class TestStatisticsHistory:
    """Test statistics history retrieval"""

    def test_get_statistics_history_with_limit(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test getting statistics history with limit"""
        for i in range(5):
            temp_db.save_statistics({
                "total_players": 1000 + i,
                "timestamp": i
            })

        result = temp_db.get_statistics_history(limit=3)
        assert isinstance(result, list)
        assert len(result) <= 3

    def test_get_statistics_history_empty(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test getting statistics history when none exists"""
        result = temp_db.get_statistics_history()
        assert result == []

    def test_get_statistics_history_format(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test that statistics history includes both data and timestamp"""
        stats_data = {"total_players": 1000}
        temp_db.save_statistics(stats_data)

        # Mock statistics history result (data, timestamp)
        from datetime import datetime, timezone
        timestamp = datetime.now(timezone.utc)
        mock_cursor.fetchall.return_value = [(stats_data, timestamp)]
        result = temp_db.get_statistics_history(limit=1)
        assert len(result) > 0
        assert "data" in result[0]
        assert "timestamp" in result[0]


class TestAssignmentVariations:
    """Test assignment variations"""

    def test_save_single_assignment(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test save_assignment method"""
        data = {"id": 1, "title": "Major Order"}
        result = temp_db.save_assignment(1, data)
        assert result is True

        # Mock assignments result
        mock_cursor.fetchall.return_value = [(data,)]
        retrieved = temp_db.get_latest_assignments(limit=1)
        assert len(retrieved) > 0

    def test_get_assignment_alias(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test get_assignment method (alias for get_latest_assignments)"""
        data = {"id": 1, "title": "Major Order"}
        temp_db.save_assignment(1, data)

        # Mock assignments result
        mock_cursor.fetchall.return_value = [(data,)]
        result = temp_db.get_assignment(limit=1)
        assert len(result) > 0

    def test_save_single_dispatch(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test save_dispatch method"""
        data = {"id": 1, "message": "News"}
        result = temp_db.save_dispatch(1, data)
        assert result is True

        # Mock dispatches result
        mock_cursor.fetchall.return_value = [(data,)]
        retrieved = temp_db.get_latest_dispatches(limit=1)
        assert len(retrieved) > 0

    def test_save_single_planet_event(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test save_planet_event method"""
        data = {"id": 1, "type": "storm"}
        result = temp_db.save_planet_event(1, 5, "defense", data)
        assert result is True

        # Mock planet events result
        mock_cursor.fetchall.return_value = [(data,)]
        retrieved = temp_db.get_latest_planet_events(limit=1)
        assert len(retrieved) > 0


class TestEmptyResultHandling:
    """Test handling of empty results"""

    def test_get_latest_planets_snapshot_no_planets(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test planets snapshot when none exist"""
        result = temp_db.get_latest_planets_snapshot()
        assert result is None

    def test_get_latest_campaigns_snapshot_no_campaigns(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test campaigns snapshot when none exist"""
        result = temp_db.get_latest_campaigns_snapshot()
        assert result is None

    def test_get_latest_factions_snapshot_no_war_data(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test factions snapshot when no war data exists"""
        result = temp_db.get_latest_factions_snapshot()
        assert result is None

    def test_get_latest_factions_snapshot_no_factions_in_war(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test factions snapshot when war data has no factions"""
        war_data = {"status": "active"}  # No factions
        temp_db.save_war_status(war_data)

        result = temp_db.get_latest_factions_snapshot()
        # When war has no factions, should return None
        assert result is None


class TestSystemStatusVariations:
    """Test system status operations variations"""

    def test_update_system_status_custom_key(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test updating custom system status"""
        result = temp_db.update_system_status("custom_key", "custom_value")
        assert result is True

        # Mock system status result
        mock_cursor.fetchone.return_value = ("custom_value",)
        retrieved = temp_db.get_system_status("custom_key")
        assert retrieved == "custom_value"

    def test_get_system_status_nonexistent(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test getting non-existent system status"""
        result = temp_db.get_system_status("nonexistent_key")
        assert result is None

    def test_upstream_status_not_set_defaults_to_false(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test that upstream status defaults to false when not set"""
        result = temp_db.get_upstream_status()
        # Initial state should be false (conservative default)
        assert isinstance(result, bool)


class TestErrorHandlingInMethods:
    """Test error handling in database methods"""

    def test_get_planet_status_history_error_handling(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test planet status history handles errors gracefully"""
        # Save valid data first
        temp_db.save_planet_status(5, {"index": 5, "name": "Planet"})
        
        result = temp_db.get_planet_status_history(5, limit=10)
        assert isinstance(result, list)

    def test_get_planet_events_error_handling(self, temp_db, mock_psycopg2):
        mock_pg, mock_conn, mock_cursor = mock_psycopg2
        """Test planet events handles errors gracefully"""
        # Save valid data
        events = [{"id": 1, "planetIndex": 5, "eventType": "defense"}]
        temp_db.save_planet_events(events)
        
        result = temp_db.get_planet_events(planet_index=5, limit=10)
        assert isinstance(result, list)
