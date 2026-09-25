package pe.edu.upeu.ec.pago.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventoPago {

    private String tipoEvento;
    private Long ordenId;
    private Double monto;
    private String estado;
    private String origen;
    private Long timestamp;
}
