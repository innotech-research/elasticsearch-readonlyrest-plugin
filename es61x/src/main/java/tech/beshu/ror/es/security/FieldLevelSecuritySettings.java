package tech.beshu.ror.es.security;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import tech.beshu.ror.commons.settings.SettingsMalformedException;
import tech.beshu.ror.commons.shims.es.ESContext;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class FieldLevelSecuritySettings {
  private final ImmutableSet<String> flsFields;
  private  boolean isBlackList = false;

  FieldLevelSecuritySettings(Optional<Set<String>> fields) {
    assert fields != null;
    System.out.println("LOOLOLOLOL " + fields);
    if(fields == null){
      flsFields = ImmutableSet.of();
      return;
    }
    Set<String> tmpFields = Sets.newHashSet();
    fields.ifPresent(fs -> {
      for (String field : fs) {
        boolean isBL = field.startsWith("!") || field.startsWith("~");
        if(isBlackList && isBL != isBlackList){
          throw new SettingsMalformedException("can't mix black list with white list in field level security");
        }
        isBlackList = isBL;
        tmpFields.add(field);
      }
    });
    flsFields = ImmutableSet.copyOf(tmpFields);
  }

  public ImmutableSet<String> getFlsFields() {
    return flsFields;
  }

  public boolean isBlackList() {
    return isBlackList;
  }
}
