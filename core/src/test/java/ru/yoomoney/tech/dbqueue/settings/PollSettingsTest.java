package ru.yoomoney.tech.dbqueue.settings;

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class PollSettingsTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void should_define_correct_equals_hashcode() {
        EqualsVerifier.forClass(PollSettings.class)
                .withIgnoredFields("observers")
                .suppress(Warning.NONFINAL_FIELDS)
                .usingGetClass().verify();
    }

    @Test
    public void should_set_value() {
        PollSettings oldValue = PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(1))
                .withNoTaskTimeout(Duration.ofSeconds(2)).withFatalCrashTimeout(Duration.ofSeconds(3))
                .withBatchSize(1).withQueryVersion(0).build();
        PollSettings newValue = PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(4))
                .withNoTaskTimeout(Duration.ofSeconds(5)).withFatalCrashTimeout(Duration.ofSeconds(6))
                .withBatchSize(2).withQueryVersion(1).build();
        Optional<String> diff = oldValue.setValue(newValue);
        assertThat(diff, equalTo(Optional.of("pollSettings(betweenTaskTimeout=PT4S<PT1S,noTaskTimeout=PT5S<PT2S,fatalCrashTimeout=PT6S<PT3S,batchSize=2<1,queryVersion=1<0)")));
        assertThat(oldValue, equalTo(newValue));
    }

    @Test
    public void should_not_allow_batch_size_lesser_than_one() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(equalTo("batchSize must not be less then 1"));
        PollSettings.builder().withBetweenTaskTimeout(Duration.ofSeconds(4))
                .withNoTaskTimeout(Duration.ofSeconds(5)).withFatalCrashTimeout(Duration.ofSeconds(6))
                .withBatchSize(0).withQueryVersion(0).build();
    }
}