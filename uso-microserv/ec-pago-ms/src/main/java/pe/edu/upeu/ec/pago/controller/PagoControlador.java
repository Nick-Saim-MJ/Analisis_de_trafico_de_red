package pe.edu.upeu.ec.pago.controller;

import pe.edu.upeu.ec.pago.entity.Pago;
import pe.edu.upeu.ec.pago.service.PagoServicio;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/pagos")
@RequiredArgsConstructor
public class PagoControlador {

    private final PagoServicio pagoServicio;

    @GetMapping("/saludo")
    public String saludo() {
        return "ec-pago-ms activo";
    }

    @GetMapping
    public List<Pago> listarPagos() {
        return pagoServicio.listarPagos();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Pago> buscarPagoPorId(@PathVariable Long id) {
        return pagoServicio.buscarPagoPorId(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
