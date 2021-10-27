package ar.juanedu.pocs.util;

import lombok.*;
import org.springframework.lang.Nullable;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Foo {

    private String s;
    private int counter;
    private String processor;
    private String totalSize;
    private String auth;
}
